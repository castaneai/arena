package arenaredis

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"sync"

	"github.com/redis/rueidis"

	"github.com/castaneai/arena"
)

type container struct {
	fleetName string
	address   string
	client    rueidis.Client
	keyPrefix string
	stopCtx   context.Context
	stopFunc  context.CancelFunc
	limiter   *capacityLimiter
}

func newContainer(client rueidis.Client, keyPrefix string, req arena.NewContainerRequest) *container {
	stopCtx, stopFunc := context.WithCancel(context.Background())
	return &container{
		fleetName: req.FleetName,
		address:   req.Address,
		client:    client,
		keyPrefix: keyPrefix,
		stopCtx:   stopCtx,
		stopFunc:  stopFunc,
		limiter:   newCapacityLimiter(req.Capacity),
	}
}

func (c *container) stop() {
	c.stopFunc()
}

func (c *container) release() {
	c.limiter.Release()
}

// start uses Redis Streams to receive events occurring in a specific Room in realtime.
func (c *container) start(req arena.NewContainerRequest) (<-chan arena.AllocatedRoom, error) {
	ch := make(chan arena.AllocatedRoom, 1024) // TODO: buffer size option
	stream := redisKeyFleetStream(c.keyPrefix, req.FleetName)
	consumerGroup := redisFleetConsumerGroup(c.keyPrefix, req.FleetName)
	cmd := c.client.B().XgroupCreate().Key(stream).Group(consumerGroup).Id("$").Mkstream().Build()
	if err := c.client.Do(c.stopCtx, cmd).Error(); err != nil && !rueidis.IsRedisBusyGroup(err) {
		return nil, err
	}

	consumerName := req.Address
	go func() {
		for {
			if err := c.limiter.Acquire(c.stopCtx); err != nil {
				return
			}
			cmd := c.client.B().Xreadgroup().Group(consumerGroup, consumerName).Block(0).Streams().Key(stream).Id(">").Build()
			reply, err := c.client.Do(c.stopCtx, cmd).AsXRead()
			if err != nil {
				if c.stopCtx.Err() != nil {
					return
				}
				if !rueidis.IsRedisNil(err) {
					err = fmt.Errorf("failed to XREADGROUP from stream: %w", err)
					slog.Error(err.Error(), "error", err)
				}
				continue
			}
			for _, entry := range reply[stream] {
				room, err := c.onAllocationEventEntry(stream, consumerName, entry)
				if err != nil {
					if c.stopCtx.Err() != nil {
						return
					}
					slog.Error(err.Error(), "error", err)
					continue
				}
				select {
				case ch <- *room:
				default:
					slog.Warn(fmt.Sprintf("allocation channel is full, dropping room allocation: %+v", room))
				}
			}
		}
	}()
	return ch, nil
}

func (c *container) onAllocationEventEntry(stream, consumerGroup string, entry rueidis.XRangeEntry) (*arena.AllocatedRoom, error) {
	defer func() {
		cmd := c.client.B().Xack().Key(stream).Group(consumerGroup).Id(entry.ID).Build()
		c.client.Do(c.stopCtx, cmd)
	}()
	requestID, ok := entry.FieldValues[streamFieldRequestID]
	if !ok {
		return nil, fmt.Errorf("missing request ID")
	}
	room, err := decodeRoomAllocationEvent(entry)
	if err != nil {
		return nil, fmt.Errorf("failed to decode room allocated room: %w", err)
	}
	if err := c.onAllocateEvent(c.stopCtx, requestID, room); err != nil {
		return nil, fmt.Errorf("failed to receive room allocated room: %w", err)
	}
	return room, nil
}

func (c *container) onAllocateEvent(ctx context.Context, requestID string, room *arena.AllocatedRoom) error {
	ack := roomAllocationAck{
		roomID:  room.RoomID,
		address: c.address,
	}
	pubSubKey := redisPubSubKeyAllocationAck(c.keyPrefix, c.fleetName, requestID)
	cmd := c.client.B().Publish().Channel(pubSubKey).Message(ack.Serialize()).Build()
	if err := c.client.Do(ctx, cmd).Error(); err != nil {
		return fmt.Errorf("failed to publish allocation ack to pubsub channel '%s': %w", pubSubKey, err)
	}
	return nil
}

func decodeRoomAllocationEvent(entry rueidis.XRangeEntry) (*arena.AllocatedRoom, error) {
	roomID, ok := entry.FieldValues[streamFieldRoomID]
	if !ok {
		return nil, fmt.Errorf("room allocated but RoomID is missing: %+v", entry)
	}
	room := &arena.AllocatedRoom{
		RoomID: roomID,
	}
	if roomInitialDataStr, ok := entry.FieldValues[streamFieldRoomInitialData]; ok {
		roomInitialData, err := base64.StdEncoding.DecodeString(roomInitialDataStr)
		if err != nil {
			return nil, fmt.Errorf("failed to decode room initial data from base64('%s'): %w", roomInitialDataStr, err)
		}
		room.RoomInitialData = roomInitialData
	}
	return room, nil
}

type capacityLimiter struct {
	initialCapacity int
	capacity        int
	mu              sync.RWMutex
	releaseCh       chan struct{}
}

func newCapacityLimiter(initialCapacity int) *capacityLimiter {
	return &capacityLimiter{initialCapacity: initialCapacity, capacity: initialCapacity, mu: sync.RWMutex{}, releaseCh: make(chan struct{})}
}

func (l *capacityLimiter) Acquire(ctx context.Context) error {
	l.mu.Lock()
	if l.capacity > 0 {
		l.capacity--
		l.mu.Unlock()
		return nil
	}
	l.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-l.releaseCh:
		return nil
	}
}

func (l *capacityLimiter) Release() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.capacity++
	if l.capacity > l.initialCapacity {
		return
	}

	select {
	case l.releaseCh <- struct{}{}:
	default:
	}
}

package arenaredis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/rueidis"

	"github.com/castaneai/arena"
)

const (
	defaultAllocationChannelBufferSize = 1024
)

type container struct {
	containerID string
	fleetName   string
	client      rueidis.DedicatedClient
	keyPrefix   string
	stopCtx     context.Context
	stopFunc    context.CancelFunc
}

func newContainer(client rueidis.Client, keyPrefix string, req arena.AddContainerRequest) *container {
	stopCtx, stopFunc := context.WithCancel(context.Background())
	dc, releaseDedicatedClient := client.Dedicate()
	return &container{
		containerID: req.ContainerID,
		fleetName:   req.FleetName,
		client:      dc,
		keyPrefix:   keyPrefix,
		stopCtx:     stopCtx,
		stopFunc: func() {
			releaseDedicatedClient()
			stopFunc()
		},
	}
}

func (c *container) stop() {
	c.stopFunc()
}

// start receives events occurring in a specific Room in realtime.
func (c *container) start() (<-chan arena.ToContainerEvent, error) {
	ch := make(chan arena.ToContainerEvent, defaultAllocationChannelBufferSize)
	channel := redisPubSubChannelContainer(c.keyPrefix, c.fleetName, c.containerID)
	received, err := subscribe(c.stopCtx, c.client, channel)
	if err != nil {
		return nil, err
	}
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-c.stopCtx.Done():
				return
			case msg := <-received:
				ev, err := decodeToContainerEvent(msg)
				if err != nil {
					slog.Error(fmt.Sprintf("failed to decode allocation event: %v", err))
					return
				}
				select {
				case ch <- ev:
				default:
					slog.Error(fmt.Sprintf("toContainer event received but channel is full: %+v", ev))
				}
			case <-ticker.C:
				expired, err := c.isExpired(c.stopCtx)
				if err != nil {
					slog.WarnContext(c.stopCtx, fmt.Sprintf("failed to check if container is expired: %+v", err), "error", err)
					continue
				}
				if expired {
					slog.DebugContext(c.stopCtx, fmt.Sprintf("container '%s' is expired, stopping...", c.containerID))
					c.stop()
					return
				}
			}
		}
	}()
	return ch, nil
}

func (c *container) isExpired(ctx context.Context) (bool, error) {
	key := redisKeyContainerHeartbeat(c.keyPrefix, c.fleetName, c.containerID)
	cmd := c.client.B().Exists().Key(key).Build()
	res := c.client.Do(ctx, cmd)
	if err := res.Error(); err != nil {
		if rueidis.IsRedisNil(err) {
			return true, nil
		}
		return false, fmt.Errorf("failed to check if container heartbeat exists: %w", err)
	}
	existsCount, err := res.AsInt64()
	if err != nil {
		return false, fmt.Errorf("failed to parse exists count as int64: %w", err)
	}
	return existsCount == 0, nil
}

func (c *container) getHeartbeatTTL(ctx context.Context) (time.Duration, error) {
	key := redisKeyContainerHeartbeat(c.keyPrefix, c.fleetName, c.containerID)
	cmd := c.client.B().Get().Key(key).Build()
	res := c.client.Do(ctx, cmd)
	if err := res.Error(); err != nil {
		if rueidis.IsRedisNil(err) {
			return 0, arena.NewError(arena.ErrorStatusNotFound, errors.New("container not found"))
		}
		return 0, fmt.Errorf("failed to get heartbeat for container: %w", err)
	}
	heartbeatValue, err := res.ToString()
	if err != nil {
		return 0, fmt.Errorf("failed to parse heartbeat value: %w", err)
	}
	return decodeHeartbeatTTLValue(heartbeatValue)
}

func (c *container) refreshTTL(ctx context.Context) error {
	ttl, err := c.getHeartbeatTTL(ctx)
	if err != nil {
		return fmt.Errorf("failed to get heartbeat TTL for container '%s': %w", c.containerID, err)
	}
	key := redisKeyContainerHeartbeat(c.keyPrefix, c.fleetName, c.containerID)
	cmd := c.client.B().Set().Key(key).Value(encodeHeartbeatTTLValue(ttl)).Ex(ttl).Build()
	res := c.client.Do(ctx, cmd)
	if err := res.Error(); err != nil {
		return fmt.Errorf("failed to refresh TTL for container '%s': %w", c.containerID, err)
	}
	return nil
}

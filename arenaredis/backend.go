package arenaredis

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"

	"github.com/redis/rueidis"

	"github.com/castaneai/arena"
)

type backend struct {
	shutdownCtx context.Context
	keyPrefix   string
	client      rueidis.Client
}

func NewBackend(shutdownCtx context.Context, keyPrefix string, client rueidis.Client) arena.Backend {
	return &backend{shutdownCtx: shutdownCtx, keyPrefix: keyPrefix, client: client}
}

func (b *backend) AddRoomGroup(ctx context.Context, req arena.AddRoomGroupRequest) (*arena.AddRoomGroupResponse, error) {
	if req.Address == "" {
		return nil, arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing address"))
	}
	if req.FleetName == "" {
		return nil, arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing fleet name"))
	}
	if req.Capacity <= 0 {
		return nil, arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("invalid capacity"))
	}

	eventCh := b.listenRoomGroupEvents(req)
	key := redisKeyAvailableRoomGroups(b.keyPrefix, req.FleetName)
	cmd := b.client.B().Zadd().Key(key).ScoreMember().ScoreMember(float64(req.Capacity), req.Address).Build()
	if err := b.client.Do(ctx, cmd).Error(); err != nil {
		return nil, arena.NewError(arena.ErrorStatusUnknown, err)
	}
	return &arena.AddRoomGroupResponse{
		EventChannel: eventCh,
	}, nil
}

// listenRoomGroupEvents uses Redis Streams to receive events occurring in a specific Room in realtime.
func (b *backend) listenRoomGroupEvents(req arena.AddRoomGroupRequest) <-chan arena.RoomGroupEvent {
	ch := make(chan arena.RoomGroupEvent, 1024) // TODO: buffer size option
	stream := redisKeyRoomGroupEventStream(b.keyPrefix, req.FleetName, req.Address)
	go func() {
		lastID := "0"
		for {
			// https://redis.io/docs/latest/develop/data-types/streams/#listening-for-new-items-with-xread
			cmd := b.client.B().Xread().Block(0).Streams().Key(stream).Id(lastID).Build()
			reply, err := b.client.Do(b.shutdownCtx, cmd).AsXRead()
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				if !rueidis.IsRedisNil(err) {
					err = fmt.Errorf("failed to parse XREAD reply as Map: %w", err)
					slog.Error(err.Error(), "error", err)
				}
				continue
			}
			for _, entry := range reply[stream] {
				lastID = entry.ID
				eventName, ok := entry.FieldValues[redisStreamFieldNameEventName]
				if !ok {
					continue
				}
				switch eventName {
				case arena.EventNameRoomAllocated:
					ev, err := decodeRoomAllocatedEvent(entry)
					if err != nil {
						err = fmt.Errorf("failed to decode room allocated event: %w", err)
						slog.Error(err.Error(), "error", err)
						continue
					}
					ch <- ev
				}
			}
		}
	}()
	return ch
}

func decodeRoomAllocatedEvent(entry rueidis.XRangeEntry) (*arena.RoomGroupEventRoomAllocated, error) {
	roomID, ok := entry.FieldValues[redisStreamFieldNameRoomID]
	if !ok {
		return nil, fmt.Errorf("room allocated but RoomID is missing: %+v", entry)
	}
	ev := &arena.RoomGroupEventRoomAllocated{
		RoomID: roomID,
	}
	if roomInitialDataStr, ok := entry.FieldValues[redisStreamFieldNameRoomInitialData]; ok {
		roomInitialData, err := base64.StdEncoding.DecodeString(roomInitialDataStr)
		if err != nil {
			return nil, fmt.Errorf("failed to decode room initial data from base64('%s'): %w", roomInitialDataStr, err)
		}
		ev.RoomInitialData = roomInitialData
	}
	return ev, nil
}

func (b *backend) DeleteRoomGroup(ctx context.Context, req arena.DeleteRoomGroupRequest) error {
	if req.Address == "" {
		return arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing address"))
	}
	if req.FleetName == "" {
		return arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing fleet name"))
	}
	key := redisKeyAvailableRoomGroups(b.keyPrefix, req.FleetName)
	cmd := b.client.B().Zrem().Key(key).Member(req.Address).Build()
	if err := b.client.Do(ctx, cmd).Error(); err != nil {
		return err
	}
	if err := b.deleteRoomGroupEventStream(ctx, req); err != nil {
		return fmt.Errorf("failed to delete room group event stream: %w", err)
	}
	return nil
}

func (b *backend) deleteRoomGroupEventStream(ctx context.Context, req arena.DeleteRoomGroupRequest) error {
	stream := redisKeyRoomGroupEventStream(b.keyPrefix, req.FleetName, req.Address)
	cmd := b.client.B().Del().Key(stream).Build()
	if err := b.client.Do(ctx, cmd).Error(); err != nil {
		return err
	}
	return nil
}

func (b *backend) SetRoomResult(ctx context.Context, req arena.SetRoomResultRequest) error {
	if req.RoomID == "" {
		return arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing room id"))
	}
	if req.ResultDataTTL == 0 {
		return arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing result data ttl"))
	}
	if req.RoomResultData == nil {
		return arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing room result data"))
	}
	key := redisKeyRoomResult(b.keyPrefix, req.RoomID)
	cmd := b.client.B().Set().Key(key).Value(rueidis.BinaryString(req.RoomResultData)).Ex(req.ResultDataTTL).Build()
	if err := b.client.Do(ctx, cmd).Error(); err != nil {
		return arena.NewError(arena.ErrorStatusUnknown, fmt.Errorf("failed to set room result to redis: %w", err))
	}
	return nil
}

func (b *backend) FreeRoom(ctx context.Context, req arena.FreeRoomRequest) error {
	if req.Address == "" {
		return arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing address"))
	}
	if req.FleetName == "" {
		return arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing fleet name"))
	}

	key := redisKeyAvailableRoomGroups(b.keyPrefix, req.FleetName)
	cmd := b.client.B().Zadd().Key(key).Xx().Incr().ScoreMember().ScoreMember(1, req.Address).Build()
	if err := b.client.Do(ctx, cmd).Error(); err != nil {
		return fmt.Errorf("failed to ZADD with INCR: %w", err)
	}
	return nil
}

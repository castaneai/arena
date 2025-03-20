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

type redisArenaSDK struct {
	keyPrefix  string
	client     rueidis.Client
	deleteCtx  context.Context
	deleteFunc context.CancelFunc
}

func NewArenaSDK(keyPrefix string, client rueidis.Client) arena.ArenaSDK {
	deleteCtx, deleteFunc := context.WithCancel(context.Background())
	return &redisArenaSDK{keyPrefix: keyPrefix, client: client, deleteFunc: deleteFunc, deleteCtx: deleteCtx}
}

func (sdk *redisArenaSDK) AddRoomGroup(ctx context.Context, req arena.AddRoomGroupRequest) (*arena.AddRoomGroupResponse, error) {
	if req.Address == "" {
		return nil, errors.New("missing address")
	}
	if req.FleetName == "" {
		return nil, errors.New("missing fleet name")
	}
	if req.Capacity <= 0 {
		return nil, errors.New("invalid capacity")
	}

	eventCh := sdk.listenEvents(req)
	key := redisKeyAvailableRoomGroups(sdk.keyPrefix, req.FleetName)
	cmd := sdk.client.B().Zadd().Key(key).ScoreMember().ScoreMember(float64(req.Capacity), req.Address).Build()
	if err := sdk.client.Do(ctx, cmd).Error(); err != nil {
		return nil, err
	}
	return &arena.AddRoomGroupResponse{
		EventChannel: eventCh,
	}, nil
}

func (sdk *redisArenaSDK) listenEvents(req arena.AddRoomGroupRequest) <-chan arena.RoomGroupEvent {
	ch := make(chan arena.RoomGroupEvent, 1024) // TODO: buffer size option
	stream := redisKeyRoomGroupEventStream(sdk.keyPrefix, req.FleetName, req.Address)
	go func() {
		lastID := "0"
		for {
			select {
			case <-sdk.deleteCtx.Done():
				return
			default:
				// https://redis.io/docs/latest/develop/data-types/streams/#listening-for-new-items-with-xread
				cmd := sdk.client.B().Xread().Block(0).Streams().Key(stream).Id(lastID).Build()
				reply, err := sdk.client.Do(sdk.deleteCtx, cmd).AsXRead()
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

func (sdk *redisArenaSDK) DeleteRoomGroup(ctx context.Context, req arena.DeleteRoomGroupRequest) error {
	if req.Address == "" {
		return errors.New("missing address")
	}
	if req.FleetName == "" {
		return errors.New("missing fleet name")
	}

	sdk.deleteFunc()
	key := redisKeyAvailableRoomGroups(sdk.keyPrefix, req.FleetName)
	cmd := sdk.client.B().Zrem().Key(key).Member(req.Address).Build()
	if err := sdk.client.Do(ctx, cmd).Error(); err != nil {
		return err
	}
	if err := sdk.deleteRoomGroupEventStream(ctx, req); err != nil {
		return fmt.Errorf("failed to delete room group event stream: %w", err)
	}
	return nil
}

func (sdk *redisArenaSDK) deleteRoomGroupEventStream(ctx context.Context, req arena.DeleteRoomGroupRequest) error {
	stream := redisKeyRoomGroupEventStream(sdk.keyPrefix, req.FleetName, req.Address)
	cmd := sdk.client.B().Del().Key(stream).Build()
	if err := sdk.client.Do(ctx, cmd).Error(); err != nil {
		return err
	}
	return nil
}

func (sdk *redisArenaSDK) FreeRoom(ctx context.Context, req arena.FreeRoomRequest) error {
	if req.Address == "" {
		return errors.New("missing address")
	}
	if req.FleetName == "" {
		return errors.New("missing fleet name")
	}

	key := redisKeyAvailableRoomGroups(sdk.keyPrefix, req.FleetName)
	cmd := sdk.client.B().Zadd().Key(key).Xx().Incr().ScoreMember().ScoreMember(1, req.Address).Build()
	if err := sdk.client.Do(ctx, cmd).Error(); err != nil {
		return fmt.Errorf("failed to ZADD with INCR: %w", err)
	}
	return nil
}

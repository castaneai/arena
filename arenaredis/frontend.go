package arenaredis

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/redis/rueidis"

	"github.com/castaneai/arena"
)

type redisFrontend struct {
	keyPrefix string
	client    rueidis.Client
}

func NewFrontend(keyPrefix string, client rueidis.Client) arena.Frontend {
	return &redisFrontend{keyPrefix: keyPrefix, client: client}
}

func (a *redisFrontend) AllocateRoom(ctx context.Context, req arena.AllocateRoomRequest) (*arena.AllocateRoomResponse, error) {
	if req.RoomID == "" {
		return nil, arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing room id"))
	}
	if req.FleetName == "" {
		return nil, arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing fleet name"))
	}

	key := redisKeyAvailableRoomGroups(a.keyPrefix, req.FleetName)
	script := rueidis.NewLuaScript(`
local key = KEYS[1]
local items = redis.call('ZRANGE', key, '(0', '+inf', 'BYSCORE', 'LIMIT', 0, 1)

if #items == 0 then
    return nil
end

local address = items[1]
redis.call('ZINCRBY', key, -1, address)
return address
`)
	reply := script.Exec(ctx, a.client, []string{key}, nil)
	roomGroupAddress, err := reply.ToString()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return nil, arena.NewError(arena.ErrorStatusResourceExhausted, errors.New("resource exhausted"))
		}
		return nil, arena.NewError(arena.ErrorStatusUnknown, fmt.Errorf("failed to exec lua script: %w", err))
	}
	if err := a.notifyRoomCreation(ctx, req, roomGroupAddress); err != nil {
		return nil, arena.NewError(arena.ErrorStatusUnknown, fmt.Errorf("failed to notify room creation: %w", err))
	}
	return &arena.AllocateRoomResponse{RoomID: req.RoomID, Address: roomGroupAddress}, nil
}

func (a *redisFrontend) GetRoomResult(ctx context.Context, req arena.GetRoomResultRequest) (*arena.GetRoomResultResponse, error) {
	if req.RoomID == "" {
		return nil, arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing room id"))
	}
	key := redisKeyRoomResult(a.keyPrefix, req.RoomID)
	cmd := a.client.B().Get().Key(key).Build()
	res := a.client.Do(ctx, cmd)
	if err := res.Error(); err != nil {
		if rueidis.IsRedisNil(err) {
			return nil, arena.NewError(arena.ErrorStatusNotFound, err)
		}
		return nil, arena.NewError(arena.ErrorStatusUnknown, fmt.Errorf("failed to get room result: %w", err))
	}
	data, err := res.AsBytes()
	if err != nil {
		return nil, arena.NewError(arena.ErrorStatusUnknown, fmt.Errorf("failed to parse redis result as bytes: %w", err))
	}
	return &arena.GetRoomResultResponse{
		RoomID:         req.RoomID,
		RoomResultData: data,
	}, nil
}

func (a *redisFrontend) notifyRoomCreation(ctx context.Context, req arena.AllocateRoomRequest, address string) error {
	stream := redisKeyRoomGroupEventStream(a.keyPrefix, req.FleetName, address)
	roomInitialDataStr := base64.StdEncoding.EncodeToString(req.RoomInitialData)
	cmd := a.client.B().Xadd().Key(stream).Id("*").FieldValue().
		FieldValue(redisStreamFieldNameEventName, arena.EventNameRoomAllocated).
		FieldValue(redisStreamFieldNameRoomID, req.RoomID).
		FieldValue(redisStreamFieldNameRoomInitialData, roomInitialDataStr).Build()
	if err := a.client.Do(ctx, cmd).Error(); err != nil {
		return fmt.Errorf("failed to XADD room event to stream '%s': %w", stream, err)
	}
	return nil
}

package arenaredis

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/rueidis"

	"github.com/castaneai/arena"
)

var (
	allocateRoomScript = rueidis.NewLuaScript(`
local key = KEYS[1]
local items = redis.call('ZRANGE', key, '(0', '+inf', 'BYSCORE', 'LIMIT', 0, 1)

if #items == 0 then
    return nil
end

local container_address = items[1]
redis.call('ZINCRBY', key, -1, container_address)
local channel = ARGV[1] .. container_address
local allocation_event = ARGV[2]
redis.call('PUBLISH', channel, allocation_event)
return container_address
`)
)

type redisFrontend struct {
	keyPrefix string
	client    rueidis.Client
	options   *redisFrontendOptions
}

type RedisFrontendOption interface {
	apply(*redisFrontendOptions)
}

type redisFrontendOptions struct {
}

func newRedisFrontendOptions(opts ...RedisFrontendOption) *redisFrontendOptions {
	options := &redisFrontendOptions{}
	for _, opt := range opts {
		opt.apply(options)
	}
	return options
}

type redisFrontendOptionFunc func(*redisFrontendOptions)

func (f redisFrontendOptionFunc) apply(options *redisFrontendOptions) {
	f(options)
}

func NewFrontend(keyPrefix string, client rueidis.Client, opts ...RedisFrontendOption) arena.Frontend {
	options := newRedisFrontendOptions(opts...)
	return &redisFrontend{keyPrefix: keyPrefix, client: client, options: options}
}

func (a *redisFrontend) AllocateRoom(ctx context.Context, req arena.AllocateRoomRequest) (*arena.AllocateRoomResponse, error) {
	if req.RoomID == "" {
		return nil, arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing room id"))
	}
	if req.FleetName == "" {
		return nil, arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing fleet name"))
	}

	containerAddress, err := a.allocateRoom(ctx, req)
	if err != nil {
		return nil, err
	}
	return &arena.AllocateRoomResponse{RoomID: req.RoomID, Address: containerAddress}, nil
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

func (a *redisFrontend) allocateRoom(ctx context.Context, req arena.AllocateRoomRequest) (string, error) {
	key := redisKeyAvailableContainersIndex(a.keyPrefix, req.FleetName)
	channelPrefix := redisPubSubChannelContainerPrefix(a.keyPrefix, req.FleetName)
	allocationEvent, err := encodeRoomAllocationEvent(arena.AllocationEvent{RoomID: req.RoomID, RoomInitialData: req.RoomInitialData})
	if err != nil {
		return "", arena.NewError(arena.ErrorStatusUnknown, fmt.Errorf("failed to encode allocation event: %w", err))
	}
	res := allocateRoomScript.Exec(ctx, a.client, []string{key}, []string{channelPrefix, allocationEvent})
	if err := res.Error(); err != nil {
		if rueidis.IsRedisNil(err) {
			return "", arena.NewError(arena.ErrorStatusResourceExhausted, errors.New("no available container"))
		}
		return "", arena.NewError(arena.ErrorStatusUnknown, fmt.Errorf("failed to allocate room: %w", err))
	}
	containerAddress, err := res.ToString()
	if err != nil {
		return "", arena.NewError(arena.ErrorStatusUnknown, fmt.Errorf("failed to parse redis result as string: %w", err))
	}
	return containerAddress, nil
}

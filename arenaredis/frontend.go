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
local room_container_key = KEYS[1]
local container_id = redis.call('GET', room_container_key)
if container_id then
	return container_id
end

local available_containers_key = KEYS[2]
local heartbeat_prefix = KEYS[5]

-- Find containers that have vacancy in capacity
local found = redis.call('ZRANGE', available_containers_key, '(0', '+inf', 'BYSCORE')
if #found == 0 then
    return nil
end

-- Check heartbeat for each container and find first alive one
for i, candidate_id in ipairs(found) do
    local heartbeat_key = heartbeat_prefix .. candidate_id
    if redis.call('EXISTS', heartbeat_key) == 1 then
        container_id = candidate_id
        break
    else
        -- Remove dead container from available containers
        redis.call('ZREM', available_containers_key, candidate_id)
    end
end

if not container_id then
    return nil
end

local room_id = ARGV[1]
local fleet_name = ARGV[2]
redis.call('ZINCRBY', available_containers_key, -1, container_id)
redis.call('SET', room_container_key, container_id)

local container_to_rooms_key = KEYS[3] .. container_id
redis.call('SADD', container_to_rooms_key, room_id)

local container_channel = KEYS[4] .. container_id
local allocation_event = ARGV[3]
redis.call('PUBLISH', container_channel, allocation_event)
return container_id
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

	containerID, err := a.allocateRoom(ctx, req)
	if err != nil {
		return nil, err
	}
	return &arena.AllocateRoomResponse{RoomID: req.RoomID, ContainerID: containerID}, nil
}

func (a *redisFrontend) NotifyToRoom(ctx context.Context, req arena.NotifyToRoomRequest) error {
	if req.RoomID == "" {
		return arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing room id"))
	}
	if req.FleetName == "" {
		return arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing fleet name"))
	}
	if len(req.Body) == 0 {
		return arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing body"))
	}
	containerID, err := a.getContainerIDByRoom(ctx, req.FleetName, req.RoomID)
	if err != nil {
		return arena.NewError(arena.ErrorStatusUnknown, fmt.Errorf("failed to parse redis result as string: %w", err))
	}
	channel := redisPubSubChannelContainer(a.keyPrefix, req.FleetName, containerID)
	data, err := encodeNotifyToRoomEvent(req.RoomID, req.Body)
	if err != nil {
		return arena.NewError(arena.ErrorStatusUnknown, fmt.Errorf("failed to encode NotifyToRoomEvent: %w", err))
	}
	cmd := a.client.B().Publish().Channel(channel).Message(data).Build()
	if err := a.client.Do(ctx, cmd).Error(); err != nil {
		return arena.NewError(arena.ErrorStatusUnknown, fmt.Errorf("failed to publish message to container: %w", err))
	}
	return nil
}

func (a *redisFrontend) allocateRoom(ctx context.Context, req arena.AllocateRoomRequest) (string, error) {
	allocationEvent, err := encodeAllocationEvent(req.RoomID, req.RoomInitialData)
	if err != nil {
		return "", arena.NewError(arena.ErrorStatusUnknown, fmt.Errorf("failed to encode allocation event: %w", err))
	}
	res := allocateRoomScript.Exec(ctx, a.client, []string{
		redisKeyRoomToContainer(a.keyPrefix, req.FleetName, req.RoomID),
		redisKeyAvailableContainersIndex(a.keyPrefix, req.FleetName),
		redisKeyContainerToRoomsPrefix(a.keyPrefix, req.FleetName),
		redisPubSubChannelContainerPrefix(a.keyPrefix, req.FleetName),
		redisKeyContainerHeartbeatPrefix(a.keyPrefix, req.FleetName),
	}, []string{req.RoomID, req.FleetName, allocationEvent})
	if err := res.Error(); err != nil {
		if rueidis.IsRedisNil(err) {
			return "", arena.NewError(arena.ErrorStatusResourceExhausted, errors.New("no available container"))
		}
		return "", arena.NewError(arena.ErrorStatusUnknown, fmt.Errorf("failed to allocate room: %w", err))
	}
	containerID, err := res.ToString()
	if err != nil {
		return "", arena.NewError(arena.ErrorStatusUnknown, fmt.Errorf("failed to parse redis result as string: %w", err))
	}
	return containerID, nil
}

func (a *redisFrontend) getContainerIDByRoom(ctx context.Context, fleetName, roomID string) (string, error) {
	key := redisKeyRoomToContainer(a.keyPrefix, fleetName, roomID)
	cmd := a.client.B().Get().Key(key).Build()
	res := a.client.Do(ctx, cmd)
	if err := res.Error(); err != nil {
		if rueidis.IsRedisNil(err) {
			return "", arena.NewError(arena.ErrorStatusNotFound, fmt.Errorf("room %s not found in fleet %s", roomID, fleetName))
		}
		return "", arena.NewError(arena.ErrorStatusUnknown, fmt.Errorf("failed to find container id by room: %w", err))
	}
	containerID, err := res.ToString()
	if err != nil {
		return "", arena.NewError(arena.ErrorStatusUnknown, fmt.Errorf("failed to parse redis result as string: %w", err))
	}
	return containerID, nil
}

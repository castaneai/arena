package arenaredis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/redis/rueidis"

	"github.com/castaneai/arena"
)

var (
	addContainerScript = rueidis.NewLuaScript(`
local available_containers_key = KEYS[1]
local heartbeat_key = KEYS[2]
local registration_key = KEYS[3]
local container_to_rooms_key = KEYS[4]
local room_to_container_prefix = KEYS[5]
local container_id = ARGV[1]
local initial_capacity = tonumber(ARGV[2])
local ttl_seconds = tonumber(ARGV[3])
local heartbeat_value = ARGV[4]
local registration_id = ARGV[5]

-- The same registration ID means the same container incarnation, so this call is a
-- re-registration (typically a retry) of a container that is still running. Its rooms must be
-- kept: a session allocated to them has no other way to be reached.
local same_incarnation = registration_id ~= '' and redis.call('GET', registration_key) == registration_id

-- Rooms left behind by a previous incarnation can never be reached again, so they are dropped.
-- Without a registration ID a retry cannot be told apart from a restart, and the older rule
-- (clear rooms unless the container registers with no capacity) applies.
local clear_rooms
if registration_id ~= '' then
	clear_rooms = not same_incarnation
else
	clear_rooms = initial_capacity > 0
end
if clear_rooms then
	local rooms = redis.call('SMEMBERS', container_to_rooms_key)
	local deleting = {container_to_rooms_key}
	for i = 1, #rooms do
		deleting[#deleting + 1] = room_to_container_prefix .. rooms[i]
		-- Delete in batches: a single DEL of every key of a container holding many rooms would
		-- risk overflowing the Lua stack.
		if #deleting >= 256 then
			redis.call('DEL', unpack(deleting))
			deleting = {}
		end
	end
	if #deleting > 0 then
		redis.call('DEL', unpack(deleting))
	end
end

-- Capacity in the index is what is left after the rooms the container currently holds.
local capacity = initial_capacity - redis.call('SCARD', container_to_rooms_key)
if capacity < 0 then
	capacity = 0
end
redis.call('ZADD', available_containers_key, capacity, container_id)
redis.call('SET', heartbeat_key, heartbeat_value, 'EX', ttl_seconds)
if registration_id ~= '' then
	redis.call('SET', registration_key, registration_id, 'EX', ttl_seconds)
else
	redis.call('DEL', registration_key)
end
return capacity
`)
)

type redisBackend struct {
	keyPrefix string
	client    rueidis.Client
	fleets    map[string]*fleet
	mu        sync.RWMutex
}

func NewBackend(keyPrefix string, client rueidis.Client) arena.Backend {
	return &redisBackend{
		keyPrefix: keyPrefix,
		client:    client,
		fleets:    make(map[string]*fleet),
		mu:        sync.RWMutex{},
	}
}

func (b *redisBackend) AddContainer(ctx context.Context, req arena.AddContainerRequest) (*arena.AddContainerResponse, error) {
	if req.ContainerID == "" {
		return nil, arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing container id"))
	}
	if req.FleetName == "" {
		return nil, arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing fleet name"))
	}
	if req.InitialCapacity < 0 {
		return nil, arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("invalid initial capacity"))
	}

	// Use default TTL if not specified
	ttl := req.HeartbeatTTL
	if ttl <= 0 {
		ttl = arena.DefaultHeartbeatTTL
	}
	ttlSeconds := int(ttl.Seconds())

	c := newContainer(b.client, b.keyPrefix, req)
	ch, err := c.start()
	if err != nil {
		return nil, fmt.Errorf("failed to listen allocation: %w", err)
	}

	// Register the container capacity, the heartbeat and the registration ID at once, so that a
	// concurrent AllocateRoom cannot observe a capacity that does not match the rooms held.
	res := addContainerScript.Exec(ctx, b.client, []string{
		redisKeyAvailableContainersIndex(b.keyPrefix, req.FleetName),
		redisKeyContainerHeartbeat(b.keyPrefix, req.FleetName, req.ContainerID),
		redisKeyContainerRegistration(b.keyPrefix, req.FleetName, req.ContainerID),
		redisKeyContainerToRooms(b.keyPrefix, req.FleetName, req.ContainerID),
		redisKeyRoomToContainerPrefix(b.keyPrefix, req.FleetName),
	}, []string{
		req.ContainerID,
		strconv.Itoa(req.InitialCapacity),
		strconv.Itoa(ttlSeconds),
		encodeHeartbeatTTLValue(ttl),
		req.RegistrationID,
	})
	if err := res.Error(); err != nil {
		c.stop()
		return nil, arena.NewError(arena.ErrorStatusUnknown, fmt.Errorf("failed to add container to available containers index: %w", err))
	}

	flt := b.getOrCreateFleet(req.FleetName)
	flt.AddContainer(c)

	return &arena.AddContainerResponse{
		EventChannel: ch,
	}, nil
}

func (b *redisBackend) DeleteContainer(ctx context.Context, req arena.DeleteContainerRequest) error {
	if req.ContainerID == "" {
		return arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing container id"))
	}
	if req.FleetName == "" {
		return arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing fleet name"))
	}

	// remove the container from the available containers index
	cmd := b.client.B().Zrem().Key(redisKeyAvailableContainersIndex(b.keyPrefix, req.FleetName)).Member(req.ContainerID).Build()
	res := b.client.Do(ctx, cmd)
	if err := res.Error(); err != nil {
		return arena.NewError(arena.ErrorStatusUnknown, fmt.Errorf("failed to remove container from available containers index: %w", err))
	}

	if err := b.removeContainerRoomMappings(ctx, req.ContainerID, req.FleetName); err != nil {
		return arena.NewError(arena.ErrorStatusUnknown, fmt.Errorf("failed to remove container rooms: %w", err))
	}

	// Remove heartbeat and registration keys
	cleanupCmd := b.client.B().Del().
		Key(redisKeyContainerHeartbeat(b.keyPrefix, req.FleetName, req.ContainerID)).
		Key(redisKeyContainerRegistration(b.keyPrefix, req.FleetName, req.ContainerID)).
		Build()
	if err := b.client.Do(ctx, cleanupCmd).Error(); err != nil {
		return arena.NewError(arena.ErrorStatusUnknown, fmt.Errorf("failed to delete heartbeat for container '%s': %w", req.ContainerID, err))
	}

	flt := b.getOrCreateFleet(req.FleetName)
	flt.DeleteContainer(req.ContainerID)

	return nil
}

func (b *redisBackend) ReleaseRoom(ctx context.Context, req arena.ReleaseRoomRequest) error {
	if req.RoomID == "" {
		return arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing room id"))
	}
	if req.ContainerID == "" {
		return arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing container id"))
	}
	if req.FleetName == "" {
		return arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing fleet name"))
	}

	// increment the capacity of the container in the available containers index
	cmds := []rueidis.Completed{
		b.client.B().Zincrby().Key(redisKeyAvailableContainersIndex(b.keyPrefix, req.FleetName)).Increment(1).Member(req.ContainerID).Build(),
		b.client.B().Srem().Key(redisKeyContainerToRooms(b.keyPrefix, req.FleetName, req.ContainerID)).Member(req.RoomID).Build(),
		b.client.B().Del().Key(redisKeyRoomToContainer(b.keyPrefix, req.FleetName, req.RoomID)).Build(),
	}
	for _, res := range b.client.DoMulti(ctx, cmds...) {
		if err := res.Error(); err != nil {
			return arena.NewError(arena.ErrorStatusUnknown, fmt.Errorf("failed to release room: %w", err))
		}
	}
	return nil
}

func (b *redisBackend) SendHeartbeat(ctx context.Context, req arena.SendHeartbeatRequest) error {
	if req.ContainerID == "" {
		return arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing container id"))
	}
	if req.FleetName == "" {
		return arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing fleet name"))
	}

	// Refresh the heartbeat TTL directly in Redis without relying on in-memory container state.
	// This allows any API instance to handle heartbeat requests, not just the one that registered the container.
	return b.refreshHeartbeatTTL(ctx, req.FleetName, req.ContainerID)
}

// refreshHeartbeatTTL refreshes the heartbeat TTL for a container directly in Redis.
// Returns ErrorStatusNotFound if the container's heartbeat key does not exist.
func (b *redisBackend) refreshHeartbeatTTL(ctx context.Context, fleetName, containerID string) error {
	key := redisKeyContainerHeartbeat(b.keyPrefix, fleetName, containerID)

	// First, get the current heartbeat value to extract the TTL
	getCmd := b.client.B().Get().Key(key).Build()
	res := b.client.Do(ctx, getCmd)
	if err := res.Error(); err != nil {
		if rueidis.IsRedisNil(err) {
			return arena.NewError(arena.ErrorStatusNotFound, fmt.Errorf("container '%s' not found in fleet '%s'", containerID, fleetName))
		}
		return fmt.Errorf("failed to get heartbeat for container '%s': %w", containerID, err)
	}

	heartbeatValue, err := res.ToString()
	if err != nil {
		return fmt.Errorf("failed to parse heartbeat value: %w", err)
	}

	ttl, err := decodeHeartbeatTTLValue(heartbeatValue)
	if err != nil {
		return fmt.Errorf("failed to decode heartbeat TTL for container '%s': %w", containerID, err)
	}

	// Refresh the TTL. The registration ID must outlive the heartbeat by no less than the heartbeat
	// itself: once it is gone, AddContainer can no longer tell a retry from a restart.
	cmds := []rueidis.Completed{
		b.client.B().Set().Key(key).Value(encodeHeartbeatTTLValue(ttl)).Ex(ttl).Build(),
		b.client.B().Expire().Key(redisKeyContainerRegistration(b.keyPrefix, fleetName, containerID)).Seconds(int64(ttl.Seconds())).Build(),
	}
	for _, res := range b.client.DoMulti(ctx, cmds...) {
		if err := res.Error(); err != nil {
			return fmt.Errorf("failed to refresh TTL for container '%s': %w", containerID, err)
		}
	}

	return nil
}

func (b *redisBackend) getOrCreateFleet(name string) *fleet {
	b.mu.Lock()
	defer b.mu.Unlock()
	f, ok := b.fleets[name]
	if !ok {
		f = newFleet(name)
		b.fleets[name] = f
	}
	return f
}

func (b *redisBackend) removeContainerRoomMappings(ctx context.Context, containerID, fleetName string) error {
	containerToRoomsKey := redisKeyContainerToRooms(b.keyPrefix, fleetName, containerID)
	cmd := b.client.B().Smembers().Key(containerToRoomsKey).Build()
	res := b.client.Do(ctx, cmd)
	if err := res.Error(); err != nil {
		return arena.NewError(arena.ErrorStatusUnknown, fmt.Errorf("failed to get rooms for container '%s': %w", containerID, err))
	}
	rooms, err := res.AsStrSlice()
	if err != nil {
		return arena.NewError(arena.ErrorStatusUnknown, fmt.Errorf("failed to parse rooms as string slice: %w", err))
	}
	delCmd := b.client.B().Del().Key(containerToRoomsKey)
	for _, roomID := range rooms {
		delCmd.Key(redisKeyRoomToContainer(b.keyPrefix, fleetName, roomID))
	}
	return b.client.Do(ctx, delCmd.Build()).Error()
}

type fleet struct {
	name       string
	containers map[string]*container
	mu         sync.RWMutex
}

func newFleet(name string) *fleet {
	return &fleet{
		name:       name,
		containers: make(map[string]*container),
		mu:         sync.RWMutex{},
	}
}

func (f *fleet) AddContainer(c *container) {
	f.mu.Lock()
	if old, ok := f.containers[c.containerID]; ok {
		old.stop()
	}
	f.containers[c.containerID] = c
	f.mu.Unlock()
}

func (f *fleet) DeleteContainer(containerID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.containers[containerID]; ok {
		// stop listening for allocation events for the container
		c.stop()
		delete(f.containers, containerID)
	}
}


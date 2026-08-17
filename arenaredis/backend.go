package arenaredis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

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
-- A container registering with no capacity states that its slots are taken, which is the only
-- evidence of a running container left once the registration ID is unknown to arena: it was never
-- stored (an older caller), or it expired with the heartbeat. Its rooms are kept in that case too.
local clear_rooms = initial_capacity > 0 and not same_incarnation
if clear_rooms then
	local rooms = redis.call('SMEMBERS', container_to_rooms_key)
	local deleting = {container_to_rooms_key}
	for i = 1, #rooms do
		deleting[#deleting + 1] = room_to_container_prefix .. rooms[i]
		-- Delete in batches: unpack has a limit on the number of values it can push, which the
		-- keys of a container holding many rooms would reach.
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
-- A caller sending no registration ID cannot say which incarnation it is, but that is not evidence
-- that the stored one is gone. Dropping the key here would make the next registration of the very
-- same container look like a new incarnation and take its rooms away.
if registration_id ~= '' then
	redis.call('SET', registration_key, registration_id, 'EX', ttl_seconds)
end
return capacity
`)

	refreshHeartbeatScript = rueidis.NewLuaScript(`
local heartbeat_key = KEYS[1]
local registration_key = KEYS[2]
local heartbeat_value = ARGV[1]
local ttl_seconds = tonumber(ARGV[2])

-- Refreshing a heartbeat that already expired would bring back a container arena has given up on,
-- so a refresh only applies while the key is alive.
if redis.call('EXISTS', heartbeat_key) == 0 then
	return 0
end
redis.call('SET', heartbeat_key, heartbeat_value, 'EX', ttl_seconds)

-- The registration ID is written again rather than extended with EXPIRE: EXPIRE cannot bring back
-- a key that expired between the two commands, and a container that loses its registration ID has
-- every later re-registration treated as a new incarnation, which takes its rooms away.
local registration_id = redis.call('GET', registration_key)
if registration_id then
	redis.call('SET', registration_key, registration_id, 'EX', ttl_seconds)
end
return 1
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
	// Redis expires keys by the second, and a TTL below that would round down to an immediate
	// expiry, which Redis rejects halfway through registering the container.
	if req.HeartbeatTTL > 0 && req.HeartbeatTTL < time.Second {
		return nil, arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("heartbeat TTL must be at least 1s"))
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
		// newContainer already took a dedicated client out of the pool.
		c.stop()
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

	// Refresh the heartbeat and the registration ID together. The two are written with the same TTL,
	// so refreshing them in one script keeps the registration ID from being lost when a refresh
	// lands just after both expired.
	refreshed := refreshHeartbeatScript.Exec(ctx, b.client, []string{
		key,
		redisKeyContainerRegistration(b.keyPrefix, fleetName, containerID),
	}, []string{encodeHeartbeatTTLValue(ttl), strconv.Itoa(int(ttl.Seconds()))})
	if err := refreshed.Error(); err != nil {
		return fmt.Errorf("failed to refresh TTL for container '%s': %w", containerID, err)
	}
	alive, err := refreshed.AsInt64()
	if err != nil {
		return fmt.Errorf("failed to parse heartbeat refresh result: %w", err)
	}
	if alive == 0 {
		return arena.NewError(arena.ErrorStatusNotFound, fmt.Errorf("container '%s' not found in fleet '%s'", containerID, fleetName))
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

// Stopping a container releases its connection, which talks to Redis and can take as long as that
// takes. Both methods below therefore only touch the map while holding the lock, and stop the
// container they replaced or removed once it is released: a slow stop then costs one caller instead
// of every AddContainer and DeleteContainer of the fleet.

func (f *fleet) AddContainer(c *container) {
	f.mu.Lock()
	old, replaced := f.containers[c.containerID]
	f.containers[c.containerID] = c
	f.mu.Unlock()
	if replaced {
		old.stop()
	}
}

func (f *fleet) DeleteContainer(containerID string) {
	f.mu.Lock()
	c, ok := f.containers[containerID]
	delete(f.containers, containerID)
	f.mu.Unlock()
	if ok {
		// stop listening for allocation events for the container
		c.stop()
	}
}

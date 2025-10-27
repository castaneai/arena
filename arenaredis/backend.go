package arenaredis

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/redis/rueidis"

	"github.com/castaneai/arena"
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
	if req.InitialCapacity <= 0 {
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

	cmds := []rueidis.Completed{
		// set (overwrite) the container capacity in the available containers index
		// Note: We overwrite with the new initial capacity, ignoring existing state
		b.client.B().Zadd().Key(redisKeyAvailableContainersIndex(b.keyPrefix, req.FleetName)).ScoreMember().ScoreMember(float64(req.InitialCapacity), req.ContainerID).Build(),
		// set initial heartbeat with TTL in value
		b.client.B().Setex().Key(redisKeyContainerHeartbeat(b.keyPrefix, req.FleetName, req.ContainerID)).Seconds(int64(ttlSeconds)).Value(encodeHeartbeatTTLValue(ttl)).Build(),
	}

	// Check if container already exists and clear allocated room mappings
	if err := b.removeContainerRoomMappings(ctx, req.ContainerID, req.FleetName); err != nil {
		return nil, arena.NewError(arena.ErrorStatusUnknown, fmt.Errorf("failed to remove container rooms: %w", err))
	}

	for _, res := range b.client.DoMulti(ctx, cmds...) {
		if err := res.Error(); err != nil {
			return nil, arena.NewError(arena.ErrorStatusUnknown, fmt.Errorf("failed to add container to available containers index: %w", err))
		}
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

	// Remove heartbeat key
	heartbeatKey := redisKeyContainerHeartbeat(b.keyPrefix, req.FleetName, req.ContainerID)
	cleanupCmd := b.client.B().Del().Key(heartbeatKey).Build()
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

	flt := b.getOrCreateFleet(req.FleetName)
	return flt.RefreshContainerTTL(ctx, req.ContainerID)
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

func (f *fleet) RefreshContainerTTL(ctx context.Context, containerID string) error {
	f.mu.RLock()
	c, ok := f.containers[containerID]
	f.mu.RUnlock()
	if !ok {
		return arena.NewError(arena.ErrorStatusNotFound, fmt.Errorf("container '%s' not found in fleet '%s'", containerID, f.name))
	}

	if err := c.refreshTTL(ctx); err != nil {
		if arena.ErrorHasStatus(err, arena.ErrorStatusNotFound) {
			f.DeleteContainer(containerID)
			return arena.NewError(arena.ErrorStatusNotFound, fmt.Errorf("container '%s' not found in fleet '%s'", containerID, f.name))
		}
		return fmt.Errorf("failed to refresh TTL for container '%s': %w", containerID, err)
	}
	return nil
}

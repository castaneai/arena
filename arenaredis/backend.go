package arenaredis

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/redis/rueidis"

	"github.com/castaneai/arena"
)

var (
	deleteContainerScript = rueidis.NewLuaScript(`
local fleet_capacities_key = KEYS[1]
local available_containers_key = KEYS[2]
local container_id = ARGV[1]
local fleet_name = ARGV[2]
local capacity = redis.call('ZSCORE', available_containers_key, container_id)
if capacity then
	redis.call('ZINCRBY', fleet_capacities_key, -capacity, fleet_name)
end
return redis.call('ZREM', available_containers_key, container_id)
`)
)

type redisBackend struct {
	keyPrefix string
	client    rueidis.Client
	fleets    map[string]*fleet
	mu        sync.RWMutex
}

func NewBackend(keyPrefix string, client rueidis.Client) arena.Backend {
	return &redisBackend{keyPrefix: keyPrefix, client: client, fleets: make(map[string]*fleet), mu: sync.RWMutex{}}
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

	cmds := []rueidis.Completed{
		// add the fleet capacity to the fleet capacities index
		b.client.B().Zincrby().Key(redisKeyFleetCapacities(b.keyPrefix)).Increment(float64(req.InitialCapacity)).Member(req.FleetName).Build(),
		// add the container to the available containers index
		b.client.B().Zincrby().Key(redisKeyAvailableContainersIndex(b.keyPrefix, req.FleetName)).Increment(float64(req.InitialCapacity)).Member(req.ContainerID).Build(),
	}
	for _, res := range b.client.DoMulti(ctx, cmds...) {
		if err := res.Error(); err != nil {
			return nil, arena.NewError(arena.ErrorStatusUnknown, fmt.Errorf("failed to add container to available containers index: %w", err))
		}
	}

	c := newContainer(b.client, b.keyPrefix, req)
	flt := b.getOrCreateFleet(req.FleetName)
	flt.AddContainer(c)
	ch, err := c.start()
	if err != nil {
		return nil, fmt.Errorf("failed to listen allocation: %w", err)
	}

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
	res := deleteContainerScript.Exec(ctx, b.client, []string{
		redisKeyFleetCapacities(b.keyPrefix),
		redisKeyAvailableContainersIndex(b.keyPrefix, req.FleetName),
	}, []string{
		req.ContainerID,
		req.FleetName,
	})
	if err := res.Error(); err != nil {
		return arena.NewError(arena.ErrorStatusUnknown, fmt.Errorf("failed to exec delete container script: %w", err))
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
		b.client.B().Zincrby().Key(redisKeyFleetCapacities(b.keyPrefix)).Increment(1).Member(req.FleetName).Build(),
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

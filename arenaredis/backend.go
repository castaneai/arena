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
local fleet_name = ARGV[1]
local container_address = ARGV[2]
local capacity = redis.call('ZSCORE', available_containers_key, container_address)
if capacity ~= nil then
	redis.call('ZINCRBY', fleet_capacities_key, -capacity, fleet_name)
end
return redis.call('ZREM', available_containers_key, container_address)
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
	if req.Address == "" {
		return nil, arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing address"))
	}
	if req.FleetName == "" {
		return nil, arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing fleet name"))
	}
	if req.Capacity <= 0 {
		return nil, arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("invalid capacity"))
	}

	cmds := []rueidis.Completed{
		// add the fleet capacity to the fleet capacities index
		b.client.B().Zincrby().Key(redisKeyFleetCapacities(b.keyPrefix)).Increment(float64(req.Capacity)).Member(req.FleetName).Build(),
		// add the container to the available containers index
		b.client.B().Zincrby().Key(redisKeyAvailableContainersIndex(b.keyPrefix, req.FleetName)).Increment(float64(req.Capacity)).Member(req.Address).Build(),
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
		AllocationChannel: ch,
	}, nil
}

func (b *redisBackend) DeleteContainer(ctx context.Context, req arena.DeleteContainerRequest) error {
	if req.Address == "" {
		return arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing address"))
	}
	if req.FleetName == "" {
		return arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing fleet name"))
	}

	// remove the container from the available containers index
	res := deleteContainerScript.Exec(ctx, b.client, []string{
		redisKeyFleetCapacities(b.keyPrefix),
		redisKeyAvailableContainersIndex(b.keyPrefix, req.FleetName),
	}, []string{
		req.FleetName,
		req.Address,
	})
	if err := res.Error(); err != nil {
		return arena.NewError(arena.ErrorStatusUnknown, fmt.Errorf("failed to exec delete container script: %w", err))
	}

	flt := b.getOrCreateFleet(req.FleetName)
	flt.DeleteContainer(req.Address)

	return nil
}

func (b *redisBackend) SetRoomResult(ctx context.Context, req arena.SetRoomResultRequest) error {
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

func (b *redisBackend) ReleaseRoom(ctx context.Context, req arena.ReleaseRoomRequest) error {
	if req.Address == "" {
		return arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing address"))
	}
	if req.FleetName == "" {
		return arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing fleet name"))
	}
	if req.ReleaseCapacity == 0 {
		return arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("release capacity must be greater than 0"))
	}

	// increment the capacity of the container in the available containers index
	cmds := []rueidis.Completed{
		b.client.B().Zincrby().Key(redisKeyFleetCapacities(b.keyPrefix)).Increment(float64(req.ReleaseCapacity)).Member(req.FleetName).Build(),
		b.client.B().Zincrby().Key(redisKeyAvailableContainersIndex(b.keyPrefix, req.FleetName)).Increment(float64(req.ReleaseCapacity)).Member(req.Address).Build(),
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
	f.containers[c.address] = c
	f.mu.Unlock()
}

func (f *fleet) DeleteContainer(addr string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.containers[addr]; ok {
		// stop listening for allocation events for the container
		c.stop()
		delete(f.containers, addr)
	}
}

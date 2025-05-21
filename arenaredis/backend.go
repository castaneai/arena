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
	keyPrefix  string
	client     rueidis.Client
	containers map[string]*container
	mu         sync.RWMutex
}

func NewBackend(keyPrefix string, client rueidis.Client) arena.Backend {
	return &redisBackend{keyPrefix: keyPrefix, client: client, containers: make(map[string]*container), mu: sync.RWMutex{}}
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

	c := newContainer(b.client, b.keyPrefix, req)
	b.mu.Lock()
	b.containers[req.Address] = c
	b.mu.Unlock()
	ch, err := c.start(req)
	if err != nil {
		return nil, fmt.Errorf("failed to listen allocation: %w", err)
	}

	// add the container to the available containers index
	key := redisKeyAvailableContainersIndex(b.keyPrefix, req.FleetName)
	cmd := b.client.B().Zadd().Key(key).Incr().ScoreMember().ScoreMember(float64(req.Capacity), req.Address).Build()
	if err := b.client.Do(ctx, cmd).Error(); err != nil {
		return nil, arena.NewError(arena.ErrorStatusUnknown, fmt.Errorf("failed to add container to available containers index: %w", err))
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
	key := redisKeyAvailableContainersIndex(b.keyPrefix, req.FleetName)
	cmd := b.client.B().Zrem().Key(key).Member(req.Address).Build()
	if err := b.client.Do(ctx, cmd).Error(); err != nil {
		return arena.NewError(arena.ErrorStatusUnknown, fmt.Errorf("failed to remove container from available containers index: %w", err))
	}

	// stop listening for allocation events for the container
	b.mu.Lock()
	defer b.mu.Unlock()
	c, ok := b.containers[req.Address]
	if ok {
		c.stop()
		delete(b.containers, req.Address)
	}
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
	key := redisKeyAvailableContainersIndex(b.keyPrefix, req.FleetName)
	cmd := b.client.B().Zadd().Key(key).Xx().Incr().ScoreMember().ScoreMember(float64(req.ReleaseCapacity), req.Address).Build()
	if err := b.client.Do(ctx, cmd).Error(); err != nil {
		return arena.NewError(arena.ErrorStatusUnknown, fmt.Errorf("failed to release room: %w", err))
	}
	return nil
}

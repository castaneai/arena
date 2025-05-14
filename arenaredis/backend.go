package arenaredis

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/redis/rueidis"

	"github.com/castaneai/arena"
)

type backend struct {
	keyPrefix  string
	client     rueidis.Client
	containers map[string]*container
	mu         sync.RWMutex
}

func NewBackend(keyPrefix string, client rueidis.Client) arena.Backend {
	return &backend{keyPrefix: keyPrefix, client: client, containers: make(map[string]*container), mu: sync.RWMutex{}}
}

func (b *backend) NewContainer(ctx context.Context, req arena.NewContainerRequest) (*arena.NewContainerResponse, error) {
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
	return &arena.NewContainerResponse{
		AllocationChannel: ch,
	}, nil
}

func (b *backend) DeleteContainer(ctx context.Context, req arena.DeleteContainerRequest) error {
	if req.Address == "" {
		return arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing address"))
	}
	if req.FleetName == "" {
		return arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing fleet name"))
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	c, ok := b.containers[req.Address]
	if ok {
		c.stop()
		delete(b.containers, req.Address)
	}
	return nil
}

func (b *backend) SetRoomResult(ctx context.Context, req arena.SetRoomResultRequest) error {
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

func (b *backend) FreeRoom(ctx context.Context, req arena.FreeRoomRequest) error {
	if req.Address == "" {
		return arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing address"))
	}
	if req.FleetName == "" {
		return arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing fleet name"))
	}

	b.mu.RLock()
	defer b.mu.RUnlock()
	if c, ok := b.containers[req.Address]; ok {
		c.release()
	}
	return nil
}

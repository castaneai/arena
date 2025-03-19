package arenaredis

import (
	"context"
	"fmt"

	"github.com/redis/rueidis"

	"github.com/castaneai/arena"
)

type redisRoomAllocator struct {
	keyPrefix string
	client    rueidis.Client
}

func NewRoomAllocator(keyPrefix string, client rueidis.Client) arena.RoomAllocator {
	return &redisRoomAllocator{keyPrefix: keyPrefix, client: client}
}

func (a *redisRoomAllocator) AllocateRoom(ctx context.Context, roomID, fleetName string) (*arena.AllocatedRoom, error) {
	key := redisKeyAvailableRoomGroups(a.keyPrefix, fleetName)
	cmd := a.client.B().Zpopmin().Key(key).Build()
	reply, err := a.client.Do(ctx, cmd).ToArray()
	if err != nil {
		return nil, fmt.Errorf("failed to parse ZPOPMIN reply (key: '%s'): %w", key, err)
	}
	if len(reply) == 0 {
		return nil, arena.ErrorRoomExhausted
	}
	if len(reply) != 2 {
		return nil, fmt.Errorf("ZPOPMIN reply expected 2 elements, got %d", len(reply))
	}
	roomGroupAddress, err := reply[0].ToString()
	if err != nil {
		return nil, fmt.Errorf("failed to parse ZPOPMIN reply[0] as string (key: '%s'): %w", key, err)
	}
	availableCount, err := reply[1].AsInt64()
	if err != nil {
		return nil, fmt.Errorf("failed to parse ZPOPMIN reply[1] as int64 (key: '%s'): %w", key, err)
	}
	// TODO: send allocate event to the room journal
	if availableCount-1 > 0 {
		cmd = a.client.B().Zadd().Key(key).ScoreMember().ScoreMember(float64(availableCount-1), roomGroupAddress).Build()
		_ = a.client.Do(ctx, cmd)
	}
	return &arena.AllocatedRoom{RoomID: roomID, Address: roomGroupAddress}, nil
}

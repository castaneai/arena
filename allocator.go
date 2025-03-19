package arena

import (
	"context"
	"errors"
)

var (
	ErrorRoomExhausted = errors.New("room exhausted")
)

type RoomAllocator interface {
	AllocateRoom(ctx context.Context, roomID, fleetName string) (*AllocatedRoom, error)
}

type AllocatedRoom struct {
	RoomID  string
	Address string
}

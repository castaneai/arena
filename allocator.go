package arena

import (
	"context"
	"errors"
)

var (
	ErrRoomExhausted = errors.New("room exhausted")
)

type RoomAllocator interface {
	// AllocateRoom searches for an available RoomGroup and allocates a Room.
	// If there is no vacancy, it returns ErrRoomExhausted.
	AllocateRoom(ctx context.Context, req AllocateRoomRequest) (*AllocateRoomResponse, error)
}

type AllocateRoomRequest struct {
	RoomID          string
	FleetName       string
	RoomInitialData []byte
}

type AllocateRoomResponse struct {
	RoomID  string
	Address string
}

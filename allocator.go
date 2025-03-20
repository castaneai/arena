package arena

import (
	"context"
	"errors"
)

var (
	ErrRoomExhausted = errors.New("room exhausted")
)

type RoomAllocator interface {
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

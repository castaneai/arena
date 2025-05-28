package arena

import (
	"context"
)

type Frontend interface {
	// AllocateRoom searches for an available containers and allocates a Room.
	// If there is no vacancy, it returns Error with code: ErrorStatusResourceExhausted.
	AllocateRoom(ctx context.Context, req AllocateRoomRequest) (*AllocateRoomResponse, error)

	// NotifyToRoom sends a message to a Room.
	// If the room does not exist, Error is returned with code: ErrorStatusNotFound.
	NotifyToRoom(ctx context.Context, req NotifyToRoomRequest) error
}

type AllocateRoomRequest struct {
	RoomID          string
	FleetName       string
	RoomInitialData []byte
}

type AllocateRoomResponse struct {
	RoomID      string
	ContainerID string
}

type NotifyToRoomRequest struct {
	RoomID    string
	FleetName string
	Body      []byte
}

package arena

import (
	"context"
)

type Frontend interface {
	// AllocateRoom searches for an available containers and allocates a Room.
	// If there is no vacancy, it returns Error with code: ErrorStatusResourceExhausted.
	AllocateRoom(ctx context.Context, req AllocateRoomRequest) (*AllocateRoomResponse, error)

	// GetRoomResult retrieves the result data of a session in a Room.
	// If the result does not exist, Error is returned with code: ErrorStatusNotFound.
	GetRoomResult(ctx context.Context, req GetRoomResultRequest) (*GetRoomResultResponse, error)
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

type GetRoomResultRequest struct {
	RoomID string
}

type GetRoomResultResponse struct {
	RoomID         string
	RoomResultData []byte
}

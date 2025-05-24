package arena

import (
	"context"
	"time"
)

type Backend interface {
	// AddContainer adds a container to arena.
	AddContainer(ctx context.Context, req AddContainerRequest) (*AddContainerResponse, error)

	// DeleteContainer removes a container from arena.
	DeleteContainer(ctx context.Context, req DeleteContainerRequest) error

	// SetRoomResult sets the result data of a session in a Room.
	SetRoomResult(ctx context.Context, req SetRoomResultRequest) error

	// ReleaseRoom releases a room and makes it available for allocation.
	ReleaseRoom(ctx context.Context, req ReleaseRoomRequest) error
}

type AddContainerRequest struct {
	ContainerID     string
	FleetName       string
	InitialCapacity int
}

type AddContainerResponse struct {
	AllocationChannel <-chan AllocationEvent
}

type AllocationEvent struct {
	RoomID          string
	RoomInitialData []byte
}

type DeleteContainerRequest struct {
	ContainerID string
	FleetName   string
}

type ReleaseRoomRequest struct {
	ContainerID string
	FleetName   string
	RoomID      string
}

type SetRoomResultRequest struct {
	RoomID         string
	RoomResultData []byte
	ResultDataTTL  time.Duration
}

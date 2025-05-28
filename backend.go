package arena

import (
	"context"
)

type Backend interface {
	// AddContainer adds a container to arena.
	AddContainer(ctx context.Context, req AddContainerRequest) (*AddContainerResponse, error)

	// DeleteContainer removes a container from arena.
	DeleteContainer(ctx context.Context, req DeleteContainerRequest) error

	// ReleaseRoom releases a room and makes it available for allocation.
	ReleaseRoom(ctx context.Context, req ReleaseRoomRequest) error
}

type AddContainerRequest struct {
	ContainerID     string
	FleetName       string
	InitialCapacity int
}

type AddContainerResponse struct {
	EventChannel <-chan ToContainerEvent
}

type ToContainerEvent interface {
	toContainerEvent()
}

type AllocationEvent struct {
	RoomID          string
	RoomInitialData []byte
}

func (e *AllocationEvent) toContainerEvent() {}

type NotifyToRoomEvent struct {
	RoomID string
	Body   []byte
}

func (e *NotifyToRoomEvent) toContainerEvent() {}

type DeleteContainerRequest struct {
	ContainerID string
	FleetName   string
}

type ReleaseRoomRequest struct {
	ContainerID string
	FleetName   string
	RoomID      string
}

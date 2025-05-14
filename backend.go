package arena

import (
	"context"
	"time"
)

type Backend interface {
	NewContainer(ctx context.Context, req NewContainerRequest) (*NewContainerResponse, error)
	DeleteContainer(ctx context.Context, req DeleteContainerRequest) error
	SetRoomResult(ctx context.Context, req SetRoomResultRequest) error
	FreeRoom(ctx context.Context, req FreeRoomRequest) error
}

type NewContainerRequest struct {
	Address   string
	FleetName string
	Capacity  int
}

type NewContainerResponse struct {
	AllocationChannel <-chan AllocatedRoom
}

type AllocatedRoom struct {
	RoomID          string
	RoomInitialData []byte
}

type DeleteContainerRequest struct {
	Address   string
	FleetName string
}

type FreeRoomRequest struct {
	Address   string
	FleetName string
}

type SetRoomResultRequest struct {
	RoomID         string
	RoomResultData []byte
	ResultDataTTL  time.Duration
}

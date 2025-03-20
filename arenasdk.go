package arena

import (
	"context"
)

const (
	EventNameRoomAllocated = "RoomAllocated"
)

type ArenaSDK interface {
	AddRoomGroup(ctx context.Context, req AddRoomGroupRequest) (*AddRoomGroupResponse, error)
	DeleteRoomGroup(ctx context.Context, req DeleteRoomGroupRequest) error
	FreeRoom(ctx context.Context, req FreeRoomRequest) error
}

type AddRoomGroupRequest struct {
	Address   string
	FleetName string
	Capacity  int
}

type AddRoomGroupResponse struct {
	EventChannel <-chan RoomGroupEvent
}

type RoomGroupEvent interface {
	RoomGroupEventName() string
}

type RoomGroupEventRoomAllocated struct {
	RoomID          string
	RoomInitialData []byte
}

func (e *RoomGroupEventRoomAllocated) RoomGroupEventName() string {
	return EventNameRoomAllocated
}

type DeleteRoomGroupRequest struct {
	Address   string
	FleetName string
}

type FreeRoomRequest struct {
	Address   string
	FleetName string
}

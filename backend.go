package arena

import (
	"context"
	"time"
)

const (
	EventNameRoomAllocated = "RoomAllocated"
)

type Backend interface {
	AddRoomGroup(ctx context.Context, req AddRoomGroupRequest) (*AddRoomGroupResponse, error)
	DeleteRoomGroup(ctx context.Context, req DeleteRoomGroupRequest) error
	SetRoomResult(ctx context.Context, req SetRoomResultRequest) error
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

type SetRoomResultRequest struct {
	RoomID         string
	RoomResultData []byte
	ResultDataTTL  time.Duration
}

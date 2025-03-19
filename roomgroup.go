package arena

import "context"

type RoomGroupManager interface {
	AddRoomGroup(ctx context.Context, req AddRoomGroupRequest) error
}

type AddRoomGroupRequest struct {
	Address   string
	Capacity  int
	FleetName string
}

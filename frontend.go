package arena

import (
	"context"
	"errors"

	"connectrpc.com/connect"

	"github.com/castaneai/arena/gen/arena"
)

type FrontendService struct {
	allocator RoomAllocator
}

func NewFrontendService(allocator RoomAllocator) *FrontendService {
	return &FrontendService{allocator: allocator}
}

func (s *FrontendService) AllocateRoom(ctx context.Context, req *connect.Request[arena.AllocateRoomRequest]) (*connect.Response[arena.AllocateRoomResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("implement me"))
}

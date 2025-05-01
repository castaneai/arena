package arenaconnect

import (
	"context"

	"connectrpc.com/connect"

	"github.com/castaneai/arena"
)

type frontendService struct {
	frontend arena.Frontend
}

func NewFrontendService(frontend arena.Frontend) FrontendServiceHandler {
	return &frontendService{frontend: frontend}
}

func (s *frontendService) AllocateRoom(ctx context.Context, req *connect.Request[AllocateRoomRequest]) (*connect.Response[AllocateRoomResponse], error) {
	resp, err := s.frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{
		RoomID:          req.Msg.RoomId,
		FleetName:       req.Msg.FleetName,
		RoomInitialData: req.Msg.RoomInitialData,
	})
	if err != nil {
		if arena.ErrorHasStatus(err, arena.ErrorStatusInvalidRequest) {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
		if arena.ErrorHasStatus(err, arena.ErrorStatusResourceExhausted) {
			return nil, connect.NewError(connect.CodeResourceExhausted, err)
		}
		return nil, err
	}
	return connect.NewResponse(&AllocateRoomResponse{
		RoomId:  resp.RoomID,
		Address: resp.Address,
	}), nil
}

func (s *frontendService) GetRoomResult(ctx context.Context, req *connect.Request[GetRoomResultRequest]) (*connect.Response[GetRoomResultResponse], error) {
	resp, err := s.frontend.GetRoomResult(ctx, arena.GetRoomResultRequest{RoomID: req.Msg.RoomId})
	if err != nil {
		if arena.ErrorHasStatus(err, arena.ErrorStatusInvalidRequest) {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
		if arena.ErrorHasStatus(err, arena.ErrorStatusNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		return nil, err
	}
	return connect.NewResponse(&GetRoomResultResponse{
		RoomId:         resp.RoomID,
		RoomResultData: resp.RoomResultData,
	}), nil
}

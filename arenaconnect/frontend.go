package arenaconnect

import (
	"context"

	"connectrpc.com/connect"

	"github.com/castaneai/arena"
	arenav1 "github.com/castaneai/arena/arenaconnect/castaneai/arena/v1"
	"github.com/castaneai/arena/arenaconnect/castaneai/arena/v1/arenav1connect"
)

type frontendService struct {
	frontend arena.Frontend
}

func NewFrontendService(frontend arena.Frontend) arenav1connect.FrontendServiceHandler {
	return &frontendService{frontend: frontend}
}

func (s *frontendService) AllocateRoom(ctx context.Context, req *connect.Request[arenav1.AllocateRoomRequest]) (*connect.Response[arenav1.AllocateRoomResponse], error) {
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
	return connect.NewResponse(&arenav1.AllocateRoomResponse{
		RoomId:  resp.RoomID,
		Address: resp.Address,
	}), nil
}

func (s *frontendService) GetRoomResult(ctx context.Context, req *connect.Request[arenav1.GetRoomResultRequest]) (*connect.Response[arenav1.GetRoomResultResponse], error) {
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
	return connect.NewResponse(&arenav1.GetRoomResultResponse{
		RoomId:         resp.RoomID,
		RoomResultData: resp.RoomResultData,
	}), nil
}

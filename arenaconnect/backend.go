package arenaconnect

import (
	"context"
	"time"

	"connectrpc.com/connect"

	"github.com/castaneai/arena"
)

type backendService struct {
	backend arena.Backend
}

func NewBackendService(backend arena.Backend) BackendServiceHandler {
	return &backendService{backend: backend}
}

func (s *backendService) AddRoomGroup(ctx context.Context, req *connect.Request[AddRoomGroupRequest], stream *connect.ServerStream[AddRoomGroupResponse]) error {
	resp, err := s.backend.AddRoomGroup(ctx, arena.AddRoomGroupRequest{
		Address:   req.Msg.Address,
		FleetName: req.Msg.FleetName,
		Capacity:  int(req.Msg.Capacity),
	})
	if err != nil {
		if arena.ErrorHasStatus(err, arena.ErrorStatusInvalidRequest) {
			return connect.NewError(connect.CodeInvalidArgument, err)
		}
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return connect.NewError(connect.CodeCanceled, ctx.Err())
		case event := <-resp.EventChannel:
			switch ev := event.(type) {
			case *arena.RoomGroupEventRoomAllocated:
				if err := stream.Send(&AddRoomGroupResponse{RoomGroupEvent: &AddRoomGroupResponse_RoomAllocated{
					RoomAllocated: &RoomGroupEventRoomAllocated{
						RoomId:          ev.RoomID,
						RoomInitialData: ev.RoomInitialData,
					},
				}}); err != nil {
					return err
				}
			}
		}
	}
}

func (s *backendService) DeleteRoomGroup(ctx context.Context, req *connect.Request[DeleteRoomGroupRequest]) (*connect.Response[DeleteRoomGroupResponse], error) {
	if err := s.backend.DeleteRoomGroup(ctx, arena.DeleteRoomGroupRequest{
		Address:   req.Msg.Address,
		FleetName: req.Msg.FleetName,
	}); err != nil {
		if arena.ErrorHasStatus(err, arena.ErrorStatusInvalidRequest) {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
		return nil, err
	}
	return &connect.Response[DeleteRoomGroupResponse]{}, nil
}

func (s *backendService) SetRoomResult(ctx context.Context, req *connect.Request[SetRoomResultRequest]) (*connect.Response[SetRoomResultResponse], error) {
	if err := s.backend.SetRoomResult(ctx, arena.SetRoomResultRequest{
		RoomID:         req.Msg.RoomId,
		RoomResultData: req.Msg.RoomResultData,
		ResultDataTTL:  time.Duration(req.Msg.ResultDataTtlSeconds * uint32(time.Second)),
	}); err != nil {
		if arena.ErrorHasStatus(err, arena.ErrorStatusInvalidRequest) {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
		return nil, err
	}
	return &connect.Response[SetRoomResultResponse]{}, nil
}

func (s *backendService) FreeRoom(ctx context.Context, req *connect.Request[FreeRoomRequest]) (*connect.Response[FreeRoomResponse], error) {
	if err := s.backend.FreeRoom(ctx, arena.FreeRoomRequest{
		Address:   req.Msg.Address,
		FleetName: req.Msg.FleetName,
	}); err != nil {
		if arena.ErrorHasStatus(err, arena.ErrorStatusInvalidRequest) {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
		return nil, err
	}
	return &connect.Response[FreeRoomResponse]{}, nil
}

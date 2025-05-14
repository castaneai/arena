package arenaredis

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"

	"github.com/castaneai/arena"
)

type redisFrontend struct {
	keyPrefix    string
	client       rueidis.Client
	pubSubClient rueidis.Client
	options      *redisFrontendOptions
}

type RedisFrontendOption interface {
	apply(*redisFrontendOptions)
}

type redisFrontendOptions struct {
	allocateTimeout time.Duration
}

func newRedisFrontendOptions(opts ...RedisFrontendOption) *redisFrontendOptions {
	options := &redisFrontendOptions{
		allocateTimeout: 1 * time.Second,
	}
	for _, opt := range opts {
		opt.apply(options)
	}
	return options
}

type redisFrontendOptionFunc func(*redisFrontendOptions)

func (f redisFrontendOptionFunc) apply(options *redisFrontendOptions) {
	f(options)
}

func NewFrontend(keyPrefix string, client, pubSubClient rueidis.Client, opts ...RedisFrontendOption) arena.Frontend {
	options := newRedisFrontendOptions(opts...)
	return &redisFrontend{keyPrefix: keyPrefix, client: client, pubSubClient: pubSubClient, options: options}
}

func (a *redisFrontend) AllocateRoom(ctx context.Context, req arena.AllocateRoomRequest) (*arena.AllocateRoomResponse, error) {
	if req.RoomID == "" {
		return nil, arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing room id"))
	}
	if req.FleetName == "" {
		return nil, arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing fleet name"))
	}

	ack, err := a.allocateRoom(ctx, req)
	if err != nil {
		return nil, err
	}
	return &arena.AllocateRoomResponse{RoomID: req.RoomID, Address: ack.address}, nil
}

func (a *redisFrontend) GetRoomResult(ctx context.Context, req arena.GetRoomResultRequest) (*arena.GetRoomResultResponse, error) {
	if req.RoomID == "" {
		return nil, arena.NewError(arena.ErrorStatusInvalidRequest, errors.New("missing room id"))
	}
	key := redisKeyRoomResult(a.keyPrefix, req.RoomID)
	cmd := a.client.B().Get().Key(key).Build()
	res := a.client.Do(ctx, cmd)
	if err := res.Error(); err != nil {
		if rueidis.IsRedisNil(err) {
			return nil, arena.NewError(arena.ErrorStatusNotFound, err)
		}
		return nil, arena.NewError(arena.ErrorStatusUnknown, fmt.Errorf("failed to get room result: %w", err))
	}
	data, err := res.AsBytes()
	if err != nil {
		return nil, arena.NewError(arena.ErrorStatusUnknown, fmt.Errorf("failed to parse redis result as bytes: %w", err))
	}
	return &arena.GetRoomResultResponse{
		RoomID:         req.RoomID,
		RoomResultData: data,
	}, nil
}

func (a *redisFrontend) allocateRoom(ctx context.Context, req arena.AllocateRoomRequest) (*roomAllocationAck, error) {
	allocateCtx, cancel := context.WithTimeout(ctx, a.options.allocateTimeout)
	defer cancel()

	requestID := uuid.New().String()
	ackCh, errCh := a.listenAllocationAck(allocateCtx, requestID, req)
	if err := a.sendAllocateRequest(allocateCtx, requestID, req); err != nil {
		return nil, fmt.Errorf("failed to send allocate request: %w", err)
	}
	select {
	case ack := <-ackCh:
		return &ack, nil
	case err := <-errCh:
		if allocateCtx.Err() != nil {
			return nil, arena.NewError(arena.ErrorStatusResourceExhausted, err)
		}
		return nil, fmt.Errorf("failed to receive allocation ack: %w", err)
	}
}

func (a *redisFrontend) listenAllocationAck(ctx context.Context, requestID string, req arena.AllocateRoomRequest) (<-chan roomAllocationAck, <-chan error) {
	ackCh := make(chan roomAllocationAck)
	errCh := make(chan error)
	go func() {
		key := redisPubSubKeyAllocationAck(a.keyPrefix, req.FleetName, requestID)
		cmd := a.pubSubClient.B().Subscribe().Channel(key).Build()
		if err := a.pubSubClient.Receive(ctx, cmd, func(msg rueidis.PubSubMessage) {
			ack, err := deserializeRoomAllocationAck(msg.Message)
			if err != nil {
				errCh <- fmt.Errorf("failed to deserialize allocation ack: %w", err)
				return
			}
			ackCh <- *ack
		}); err != nil {
			errCh <- fmt.Errorf("failed to subscribe to allocation ack: %w", err)
		}
	}()
	return ackCh, errCh
}

func (a *redisFrontend) sendAllocateRequest(ctx context.Context, requestID string, req arena.AllocateRoomRequest) error {
	streamKey := redisKeyFleetStream(a.keyPrefix, req.FleetName)
	roomInitialDataStr := base64.StdEncoding.EncodeToString(req.RoomInitialData)
	cmd := a.client.B().Xadd().Key(streamKey).Id("*").FieldValue().
		FieldValue(streamFieldRequestID, requestID).
		FieldValue(streamFieldRoomID, req.RoomID).
		FieldValue(streamFieldRoomInitialData, roomInitialDataStr).Build()
	if err := a.client.Do(ctx, cmd).Error(); err != nil {
		return fmt.Errorf("failed to XADD room event to stream '%s': %w", streamKey, err)
	}
	return nil
}

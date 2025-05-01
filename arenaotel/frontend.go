package arenaotel

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/castaneai/arena"
)

const (
	fleetNameKey          = attribute.Key("fleet_name")
	roomAllocateStatusKey = attribute.Key("status")
)

var (
	roomAllocatedStatusOK        = roomAllocateStatusKey.String("ok")
	roomAllocatedStatusExhausted = roomAllocateStatusKey.String("exhausted")
	roomAllocatedStatusError     = roomAllocateStatusKey.String("error")
)

type frontend struct {
	inner                arena.Frontend
	meterProvider        metric.MeterProvider
	roomAllocatedCount   metric.Int64Counter
	roomAllocatedLatency metric.Float64Histogram
}

func NewFrontend(inner arena.Frontend) (arena.Frontend, error) {
	meterProvider := otel.GetMeterProvider()
	meter := meterProvider.Meter(scopeName)
	roomAllocatedCount, err := meter.Int64Counter("arena.room_allocated.count_total")
	if err != nil {
		return nil, err
	}
	roomAllocatedLatency, err := meter.Float64Histogram("arena.room_allocated_latency_seconds",
		metric.WithUnit("s"), metric.WithExplicitBucketBoundaries(latencyHistogramBuckets...))
	if err != nil {
		return nil, err
	}
	return &frontend{
		inner:                inner,
		meterProvider:        meterProvider,
		roomAllocatedCount:   roomAllocatedCount,
		roomAllocatedLatency: roomAllocatedLatency,
	}, nil
}

func (a *frontend) AllocateRoom(ctx context.Context, req arena.AllocateRoomRequest) (*arena.AllocateRoomResponse, error) {
	fleetNameAttr := fleetNameKey.String(req.FleetName)
	statusAttr := roomAllocatedStatusOK
	start := time.Now()
	defer func() {
		a.roomAllocatedCount.Add(ctx, 1, metric.WithAttributes(fleetNameAttr, statusAttr))
		a.roomAllocatedLatency.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(fleetNameAttr, statusAttr))
	}()
	resp, err := a.inner.AllocateRoom(ctx, req)
	if err != nil {
		if arena.ErrorHasStatus(err, arena.ErrorStatusResourceExhausted) {
			statusAttr = roomAllocatedStatusExhausted
		} else {
			statusAttr = roomAllocatedStatusError
		}
		return nil, err
	}
	return resp, nil
}

func (a *frontend) GetRoomResult(ctx context.Context, req arena.GetRoomResultRequest) (*arena.GetRoomResultResponse, error) {
	// TODO: implement
	return a.inner.GetRoomResult(ctx, req)
}

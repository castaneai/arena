package arenaotel

import (
	"context"
	"errors"
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

type roomAllocator struct {
	inner                arena.RoomAllocator
	meterProvider        metric.MeterProvider
	roomAllocatedCount   metric.Int64Counter
	roomAllocatedLatency metric.Float64Histogram
}

func NewRoomAllocator(inner arena.RoomAllocator) (arena.RoomAllocator, error) {
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
	return &roomAllocator{
		inner:                inner,
		meterProvider:        meterProvider,
		roomAllocatedCount:   roomAllocatedCount,
		roomAllocatedLatency: roomAllocatedLatency,
	}, nil
}

func (a *roomAllocator) AllocateRoom(ctx context.Context, req arena.AllocateRoomRequest) (*arena.AllocateRoomResponse, error) {
	fleetNameAttr := fleetNameKey.String(req.FleetName)
	statusAttr := roomAllocatedStatusOK
	start := time.Now()
	defer func() {
		a.roomAllocatedCount.Add(ctx, 1, metric.WithAttributes(fleetNameAttr, statusAttr))
		a.roomAllocatedLatency.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(fleetNameAttr, statusAttr))
	}()
	resp, err := a.inner.AllocateRoom(ctx, req)
	if err != nil {
		if errors.Is(err, arena.ErrRoomExhausted) {
			statusAttr = roomAllocatedStatusExhausted
		} else {
			statusAttr = roomAllocatedStatusError
		}
		return nil, err
	}
	return resp, nil
}

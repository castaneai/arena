package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/rueidis"
	"github.com/redis/rueidis/rueidisotel"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.30.0"

	"github.com/castaneai/arena"
	"github.com/castaneai/arena/arenaredis"
)

const (
	serviceName       = "arena_loadtest"
	fleetName         = "fleet"
	redisKeyPrefix    = "arenaloadtest:"
	localRedisAddr    = "localhost:6379"
	localOtelEndpoint = "http://localhost:4317"
)

func main() {
	ctx, shutdown := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer shutdown()

	otelRes, err := resource.New(ctx, resource.WithAttributes(semconv.ServiceName(serviceName)))
	if err != nil {
		err := fmt.Errorf("failed to create otel resource: %w", err)
		slog.Error(err.Error(), "error", err)
		os.Exit(1)
	}
	exporter, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithEndpointURL(localOtelEndpoint))
	if err != nil {
		err := fmt.Errorf("failed to create OTLP metric exporter: %w", err)
		slog.Error(err.Error(), "error", err)
		os.Exit(1)
	}
	defer exporter.Shutdown(context.Background())
	otel.SetMeterProvider(metric.NewMeterProvider(
		metric.WithReader(metric.NewPeriodicReader(exporter, metric.WithInterval(10*time.Second))),
		metric.WithResource(otelRes),
	))

	redisClient, err := rueidisotel.NewClient(rueidis.ClientOption{InitAddress: []string{localRedisAddr}, DisableCache: true},
		rueidisotel.WithOperationMetricAttr())
	if err != nil {
		err := fmt.Errorf("failed to create redis client: %w", err)
		slog.Error(err.Error(), "error", err)
		os.Exit(1)
	}
	frontend := arenaredis.NewFrontend(redisKeyPrefix, redisClient)
	backend := arenaredis.NewBackend(redisKeyPrefix, redisClient)
	resp, err := backend.AddContainer(ctx, arena.AddContainerRequest{FleetName: fleetName, ContainerID: "dummy", InitialCapacity: 9999999})
	if err != nil {
		err := fmt.Errorf("failed to add container: %w", err)
		slog.Error(err.Error(), "error", err)
		return
	}
	go func() {
		for room := range resp.AllocationChannel {
			slog.Info(fmt.Sprintf("allocated: %+v", room))
		}
	}()

	slog.Info(fmt.Sprintf("arena loadtest is running..."))

	ticker := time.NewTicker(10 * time.Millisecond)
	i := 0
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			slog.Info("shutting down...")
			return
		case <-ticker.C:
			i++
			if _, err := frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: fmt.Sprintf("room%d", i), FleetName: fleetName}); err != nil {
				err := fmt.Errorf("failed to allocate room: %w", err)
				slog.Error(err.Error(), "error", err)
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
}

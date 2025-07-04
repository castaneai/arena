package arenaredis

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"

	"github.com/castaneai/arena"
)

const (
	localRedisAddr  = "localhost:6379"
	chanReadTimeout = 10 * time.Second
)

func TestAllocation(t *testing.T) {
	fleet1Name := "fleet1"
	ctx := t.Context()
	frontend, backend, _ := newFrontendBackendMetrics(t)

	_, err := frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room1", FleetName: fleet1Name})
	require.True(t, arena.ErrorHasStatus(err, arena.ErrorStatusResourceExhausted))

	// new con1: [(free), (free)] (1/2)
	con1, err := backend.AddContainer(ctx, arena.AddContainerRequest{ContainerID: "con1", InitialCapacity: 2, FleetName: fleet1Name})
	require.NoError(t, err)

	// con1: [room1, (free)] (1/2)
	room1, err := frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room1", FleetName: fleet1Name})
	require.NoError(t, err)
	require.Equal(t, "con1", room1.ContainerID)
	ev := mustReadChan(t, con1.EventChannel).(*arena.AllocationEvent)
	require.Equal(t, "room1", ev.RoomID)

	// con1: [room1, room2] (2/2)
	room2, err := frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room2", FleetName: fleet1Name, RoomInitialData: []byte("hello")})
	require.NoError(t, err)
	require.Equal(t, "con1", room2.ContainerID)
	ev = mustReadChan(t, con1.EventChannel).(*arena.AllocationEvent)
	require.Equal(t, "room2", ev.RoomID)
	require.Equal(t, "hello", string(ev.RoomInitialData))

	// con1: [room1, room2] (2/2)
	// new con2: [(free), (free)] (0/2)
	con2, err := backend.AddContainer(ctx, arena.AddContainerRequest{ContainerID: "con2", InitialCapacity: 2, FleetName: fleet1Name})
	require.NoError(t, err)

	// con1: [room1, room2] (2/2)
	// con2: [room3, (free)] (1/2)
	room3, err := frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room3", FleetName: fleet1Name})
	require.NoError(t, err)
	require.Equal(t, "con2", room3.ContainerID)
	ev = mustReadChan(t, con2.EventChannel).(*arena.AllocationEvent)
	require.Equal(t, "room3", ev.RoomID)

	// con1: [room1, room2] (2/2)
	// con2: [room3, room4] (2/2)
	room4, err := frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room4", FleetName: fleet1Name})
	require.NoError(t, err)
	require.Equal(t, "con2", room4.ContainerID)
	ev = mustReadChan(t, con2.EventChannel).(*arena.AllocationEvent)
	require.Equal(t, "room4", ev.RoomID)

	// cannot allocate because all containers are full; returns resource exhausted.
	_, err = frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room5", FleetName: fleet1Name})
	require.True(t, arena.ErrorHasStatus(err, arena.ErrorStatusResourceExhausted))

	// ReleaseRoom will again free up a slot, which can be allocated
	// con1: [room1, (free)] (1/2)
	// con2: [room3, room4] (2/2)
	err = backend.ReleaseRoom(ctx, arena.ReleaseRoomRequest{ContainerID: "con1", FleetName: fleet1Name, RoomID: "room2"})
	require.NoError(t, err)

	room5, err := frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room5", FleetName: fleet1Name})
	require.NoError(t, err)
	require.Equal(t, "con1", room5.ContainerID)
	ev = mustReadChan(t, con1.EventChannel).(*arena.AllocationEvent)
	require.Equal(t, "room5", ev.RoomID)

	err = backend.DeleteContainer(ctx, arena.DeleteContainerRequest{ContainerID: "con1", FleetName: fleet1Name})
	require.NoError(t, err)
	err = backend.DeleteContainer(ctx, arena.DeleteContainerRequest{ContainerID: "con2", FleetName: fleet1Name})
	require.NoError(t, err)
}

func TestAllocateRoomDuplicated(t *testing.T) {
	fleet1Name := "fleet1"
	ctx := t.Context()
	frontend, backend, _ := newFrontendBackendMetrics(t)

	con1, err := backend.AddContainer(ctx, arena.AddContainerRequest{ContainerID: "con1", InitialCapacity: 2, FleetName: fleet1Name})
	require.NoError(t, err)

	room1, err := frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room1", FleetName: fleet1Name})
	require.NoError(t, err)
	require.Equal(t, "con1", room1.ContainerID)
	ev := mustReadChan(t, con1.EventChannel).(*arena.AllocationEvent)
	require.Equal(t, "room1", ev.RoomID)

	// Allocate the same room again, should return the same container.
	room1, err = frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room1", FleetName: fleet1Name})
	require.NoError(t, err)
	require.Equal(t, "con1", room1.ContainerID)
	mustTimeoutChan(t, con1.EventChannel, 1*time.Second)
}

func TestNotifyToRoom(t *testing.T) {
	fleet1Name := "fleet1"
	ctx := t.Context()
	frontend, backend, _ := newFrontendBackendMetrics(t)

	con1, err := backend.AddContainer(ctx, arena.AddContainerRequest{ContainerID: "con1", InitialCapacity: 2, FleetName: fleet1Name})
	require.NoError(t, err)
	room1, err := frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room1", FleetName: fleet1Name})
	require.NoError(t, err)
	require.Equal(t, "con1", room1.ContainerID)
	ev1 := mustReadChan(t, con1.EventChannel).(*arena.AllocationEvent)
	require.Equal(t, "room1", ev1.RoomID)

	err = frontend.NotifyToRoom(ctx, arena.NotifyToRoomRequest{RoomID: "room1", FleetName: fleet1Name, Body: []byte("hello_room1")})
	require.NoError(t, err)
	ev2 := mustReadChan(t, con1.EventChannel).(*arena.NotifyToRoomEvent)
	require.Equal(t, "hello_room1", string(ev2.Body))
}

func TestHeartbeat(t *testing.T) {
	fleet1Name := "fleet1"
	ctx := t.Context()
	frontend, backend, _ := newFrontendBackendMetrics(t)

	// Test heartbeat functionality - should fail for non-existent container
	err := backend.SendHeartbeat(ctx, arena.SendHeartbeatRequest{ContainerID: "con1", FleetName: fleet1Name})
	require.True(t, arena.ErrorHasStatus(err, arena.ErrorStatusNotFound))

	// Add container with short TTL for testing
	ttl := 3 * time.Second
	con1, err := backend.AddContainer(ctx, arena.AddContainerRequest{ContainerID: "con1", InitialCapacity: 2, FleetName: fleet1Name, HeartbeatTTL: ttl})
	require.NoError(t, err)

	// Initially should be able to allocate room
	room1, err := frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room1", FleetName: fleet1Name})
	require.NoError(t, err)
	require.Equal(t, "con1", room1.ContainerID)
	ev := mustReadChan(t, con1.EventChannel).(*arena.AllocationEvent)
	require.Equal(t, "room1", ev.RoomID)

	// Wait close to TTL expiry, then send heartbeat to refresh
	time.Sleep(ttl - time.Second)
	err = backend.SendHeartbeat(ctx, arena.SendHeartbeatRequest{ContainerID: "con1", FleetName: fleet1Name})
	require.NoError(t, err)

	// Should still be able to allocate room after refresh
	room2, err := frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room2", FleetName: fleet1Name})
	require.NoError(t, err)
	require.Equal(t, "con1", room2.ContainerID)

	// Wait for heartbeat to expire (4 seconds > 3 seconds TTL since last heartbeat)
	time.Sleep(ttl + time.Second)

	// After heartbeat expires, allocation should fail (no available containers)
	_, err = frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room3", FleetName: fleet1Name})
	require.True(t, arena.ErrorHasStatus(err, arena.ErrorStatusResourceExhausted))
}

func newFrontendBackendMetrics(t *testing.T) (arena.Frontend, arena.Backend, *Metrics) {
	t.Helper()
	frontendClient, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{localRedisAddr}, DisableCache: true})
	if err != nil {
		t.Fatalf("failed to create frontend redis frontendClient: %+v", err)
	}
	checkRedisConnection(t, frontendClient)
	keyPrefix := fmt.Sprintf("arenaredis_test_%s", uuid.New().String())
	frontend := NewFrontend(keyPrefix, frontendClient)
	backendClient, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{localRedisAddr}, DisableCache: true})
	if err != nil {
		t.Fatalf("failed to create pub/sub redis frontendClient: %+v", err)
	}
	checkRedisConnection(t, backendClient)
	backend := NewBackend(keyPrefix, backendClient)
	metricsClient, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{localRedisAddr}, DisableCache: true})
	if err != nil {
		t.Fatalf("failed to create frontend redis frontendClient: %+v", err)
	}
	checkRedisConnection(t, metricsClient)
	metrics := NewMetrics(keyPrefix, metricsClient)
	return frontend, backend, metrics
}

func checkRedisConnection(t *testing.T, c rueidis.Client) {
	t.Helper()
	if err := c.Do(t.Context(), c.B().Ping().Build()).Error(); err != nil {
		t.Fatalf("failed to connect to redis: %v", err)
	}
}

func mustReadChan[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(chanReadTimeout):
		t.Fatalf("timed out waiting for channel")
	}
	panic("unreachable")
}

func mustTimeoutChan[T any](t *testing.T, ch <-chan T, timeout time.Duration) {
	t.Helper()
	select {
	case v := <-ch:
		t.Fatalf("expected timeout, but got value: %v", v)
	case <-time.After(timeout):
		return
	}
}

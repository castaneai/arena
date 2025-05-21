package arenaredis

import (
	"testing"
	"time"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"

	"github.com/castaneai/arena"
)

const (
	localRedisAddr   = "localhost:6379"
	testingKeyPrefix = "arenatest:"
	chanReadTimeout  = 10 * time.Second
)

func TestAllocation(t *testing.T) {
	fleet1Name := "fleet1"
	ctx := t.Context()
	frontend, backend := newFrontendBackend(t)

	// new con1: [(free), (free)] (1/2)
	con1, err := backend.AddContainer(ctx, arena.AddContainerRequest{Address: "con1", Capacity: 2, FleetName: fleet1Name})
	require.NoError(t, err)

	// con1: [room1, (free)] (1/2)
	room1, err := frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room1", FleetName: fleet1Name})
	require.NoError(t, err)
	require.Equal(t, "con1", room1.Address)
	ev := mustReadChan(t, con1.AllocationChannel)
	require.Equal(t, "room1", ev.RoomID)

	// con1: [room1, room2] (2/2)
	room2, err := frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room2", FleetName: fleet1Name, RoomInitialData: []byte("hello")})
	require.NoError(t, err)
	require.Equal(t, "con1", room2.Address)
	ev = mustReadChan(t, con1.AllocationChannel)
	require.Equal(t, "room2", ev.RoomID)
	require.Equal(t, "hello", string(ev.RoomInitialData))

	// con1: [room1, room2] (2/2)
	// new con2: [(free), (free)] (0/2)
	con2, err := backend.AddContainer(ctx, arena.AddContainerRequest{Address: "con2", Capacity: 2, FleetName: fleet1Name})
	require.NoError(t, err)

	// con1: [room1, room2] (2/2)
	// con2: [room3, (free)] (1/2)
	room3, err := frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room3", FleetName: fleet1Name})
	require.NoError(t, err)
	require.Equal(t, "con2", room3.Address)
	ev = mustReadChan(t, con2.AllocationChannel)
	require.Equal(t, "room3", ev.RoomID)

	// con1: [room1, room2] (2/2)
	// con2: [room3, room4] (2/2)
	room4, err := frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room4", FleetName: fleet1Name})
	require.NoError(t, err)
	require.Equal(t, "con2", room4.Address)
	ev = mustReadChan(t, con2.AllocationChannel)
	require.Equal(t, "room4", ev.RoomID)

	// cannot allocate because all containers are full; returns resource exhausted.
	_, err = frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room5", FleetName: fleet1Name})
	require.True(t, arena.ErrorHasStatus(err, arena.ErrorStatusResourceExhausted))

	// GetRoomResult returns StatusNotFound before SetRoomResult.
	_, err = frontend.GetRoomResult(ctx, arena.GetRoomResultRequest{RoomID: "room1"})
	require.True(t, arena.ErrorHasStatus(err, arena.ErrorStatusNotFound))

	// SetRoomResult creates a RoomResult.
	room1resTTL := 2 * time.Second
	err = backend.SetRoomResult(ctx, arena.SetRoomResultRequest{RoomID: "room1", RoomResultData: []byte("room1_ok"), ResultDataTTL: room1resTTL})
	require.NoError(t, err)

	// After SetRoomResult, GetRoomResult should succeed.
	room1res, err := frontend.GetRoomResult(ctx, arena.GetRoomResultRequest{RoomID: "room1"})
	require.NoError(t, err)
	require.Equal(t, "room1_ok", string(room1res.RoomResultData))

	// GetRoomResult returns StatusNotFound before SetRoomResult.
	_, err = frontend.GetRoomResult(ctx, arena.GetRoomResultRequest{RoomID: "room2"})
	require.True(t, arena.ErrorHasStatus(err, arena.ErrorStatusNotFound))

	// RoomResult is deleted when TTL is exceeded.
	time.Sleep(room1resTTL + 100*time.Millisecond)
	_, err = frontend.GetRoomResult(ctx, arena.GetRoomResultRequest{RoomID: "room1"})
	require.True(t, arena.ErrorHasStatus(err, arena.ErrorStatusNotFound))

	// ReleaseRoom will again free up a slot, which can be allocated
	// con1: [room1, (free)] (1/2)
	// con2: [room3, room4] (2/2)
	err = backend.ReleaseRoom(ctx, arena.ReleaseRoomRequest{Address: "con1", FleetName: fleet1Name, ReleaseCapacity: 1})
	require.NoError(t, err)

	room5, err := frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room5", FleetName: fleet1Name})
	require.NoError(t, err)
	require.Equal(t, "con1", room5.Address)
	ev = mustReadChan(t, con1.AllocationChannel)
	require.Equal(t, "room5", ev.RoomID)

	err = backend.DeleteContainer(ctx, arena.DeleteContainerRequest{Address: "con1", FleetName: fleet1Name})
	require.NoError(t, err)
	err = backend.DeleteContainer(ctx, arena.DeleteContainerRequest{Address: "con2", FleetName: fleet1Name})
	require.NoError(t, err)
}

func newFrontendBackend(t *testing.T) (arena.Frontend, arena.Backend) {
	t.Helper()
	frontendClient, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{localRedisAddr}, DisableCache: true})
	if err != nil {
		t.Fatalf("failed to create frontend redis frontendClient: %+v", err)
	}
	checkRedisConnection(t, frontendClient)
	frontend := NewFrontend(testingKeyPrefix, frontendClient)
	backendClient, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{localRedisAddr}, DisableCache: true})
	if err != nil {
		t.Fatalf("failed to create pub/sub redis frontendClient: %+v", err)
	}
	checkRedisConnection(t, backendClient)
	backend := NewBackend(testingKeyPrefix, backendClient)
	return frontend, backend
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

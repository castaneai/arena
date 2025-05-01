package arenaredis

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"

	"github.com/castaneai/arena"
)

const (
	testingKeyPrefix = "arenatest:"
	chanReadTimeout  = 10 * time.Second
)

func TestAllocator(t *testing.T) {
	fleet1Name := "fleet1"
	ctx := t.Context()
	client, redis := newRedisClientWithMiniRedis(t)
	backend := NewBackend(ctx, testingKeyPrefix, client)
	frontend := NewFrontend(testingKeyPrefix, client)

	// group1: (0/2)
	group1, err := backend.AddRoomGroup(ctx, arena.AddRoomGroupRequest{Address: "group1", Capacity: 2, FleetName: fleet1Name})
	require.NoError(t, err)

	// group1: (1/2)
	room1, err := frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room1", FleetName: fleet1Name})
	require.NoError(t, err)
	require.Equal(t, "group1", room1.Address)
	ev := mustReadChan(t, group1.EventChannel).(*arena.RoomGroupEventRoomAllocated)
	require.Equal(t, "room1", ev.RoomID)

	// group1: (1/2)
	// group2: (0/2)
	group2, err := backend.AddRoomGroup(ctx, arena.AddRoomGroupRequest{Address: "group2", Capacity: 2, FleetName: fleet1Name})
	require.NoError(t, err)

	// group1: (2/2)
	// group2: (0/2)
	room2, err := frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room2", FleetName: fleet1Name, RoomInitialData: []byte("hello")})
	require.NoError(t, err)
	require.Equal(t, "group1", room2.Address)
	ev = mustReadChan(t, group1.EventChannel).(*arena.RoomGroupEventRoomAllocated)
	require.Equal(t, "room2", ev.RoomID)
	require.Equal(t, "hello", string(ev.RoomInitialData))

	// group1: (2/2)
	// group2: (1/2)
	room3, err := frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room3", FleetName: fleet1Name})
	require.NoError(t, err)
	require.Equal(t, "group2", room3.Address)
	ev = mustReadChan(t, group2.EventChannel).(*arena.RoomGroupEventRoomAllocated)
	require.Equal(t, "room3", ev.RoomID)

	// all room groups are full
	// group1: (2/2)
	// group2: (2/2)
	room4, err := frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room4", FleetName: fleet1Name})
	require.NoError(t, err)
	require.Equal(t, "group2", room4.Address)
	ev = mustReadChan(t, group2.EventChannel).(*arena.RoomGroupEventRoomAllocated)
	require.Equal(t, "room4", ev.RoomID)

	// cannot allocate because all RoomGroups are full; returns resource exhausted.
	_, err = frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room5", FleetName: fleet1Name})
	require.True(t, arena.ErrorHasStatus(err, arena.ErrorStatusResourceExhausted))

	// GetRoomResult returns StatusNotFound before SetRoomResult.
	_, err = frontend.GetRoomResult(ctx, arena.GetRoomResultRequest{RoomID: "room1"})
	require.True(t, arena.ErrorHasStatus(err, arena.ErrorStatusNotFound))

	// SetRoomResult creates a RoomResult.
	room1resTTL := 10 * time.Second
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
	redis.FastForward(room1resTTL + time.Second)
	_, err = frontend.GetRoomResult(ctx, arena.GetRoomResultRequest{RoomID: "room1"})
	require.True(t, arena.ErrorHasStatus(err, arena.ErrorStatusNotFound))

	// FreeRoom will again free up a slot, which can be allocated
	// group1: (1/2)
	// group2: (2/2)
	err = backend.FreeRoom(ctx, arena.FreeRoomRequest{Address: "group1", FleetName: fleet1Name})
	require.NoError(t, err)

	room5, err := frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room5", FleetName: fleet1Name})
	require.NoError(t, err)
	require.Equal(t, "group1", room5.Address)
	ev = mustReadChan(t, group1.EventChannel).(*arena.RoomGroupEventRoomAllocated)
	require.Equal(t, "room5", ev.RoomID)

	err = backend.DeleteRoomGroup(ctx, arena.DeleteRoomGroupRequest{Address: "group1", FleetName: fleet1Name})
	require.NoError(t, err)
	err = backend.DeleteRoomGroup(ctx, arena.DeleteRoomGroupRequest{Address: "group2", FleetName: fleet1Name})
	require.NoError(t, err)
}

func newRedisClientWithMiniRedis(t *testing.T) (rueidis.Client, *miniredis.Miniredis) {
	t.Helper()
	r := miniredis.RunT(t)
	t.Cleanup(func() { r.Close() })
	client, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{r.Addr()}, DisableCache: true})
	if err != nil {
		t.Fatalf("failed to create redis client: %+v", err)
	}
	return client, r
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

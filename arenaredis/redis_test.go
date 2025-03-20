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
	client, _ := newRedisClientWithMiniRedis(t)
	sdk := NewArenaSDK(testingKeyPrefix, client)
	allocator := NewRoomAllocator(testingKeyPrefix, client)

	// group1: (0/2)
	group1, err := sdk.AddRoomGroup(ctx, arena.AddRoomGroupRequest{Address: "group1", Capacity: 2, FleetName: fleet1Name})
	require.NoError(t, err)

	// group1: (1/2)
	room1, err := allocator.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room1", FleetName: fleet1Name})
	require.NoError(t, err)
	require.Equal(t, "group1", room1.Address)
	ev := mustReadChan(t, group1.EventChannel).(*arena.RoomGroupEventRoomAllocated)
	require.Equal(t, "room1", ev.RoomID)

	// group1: (1/2)
	// group2: (0/2)
	group2, err := sdk.AddRoomGroup(ctx, arena.AddRoomGroupRequest{Address: "group2", Capacity: 2, FleetName: fleet1Name})
	require.NoError(t, err)

	// group1: (2/2)
	// group2: (0/2)
	room2, err := allocator.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room2", FleetName: fleet1Name, RoomInitialData: []byte("hello")})
	require.NoError(t, err)
	require.Equal(t, "group1", room2.Address)
	ev = mustReadChan(t, group1.EventChannel).(*arena.RoomGroupEventRoomAllocated)
	require.Equal(t, "room2", ev.RoomID)
	require.Equal(t, "hello", string(ev.RoomInitialData))

	// group1: (2/2)
	// group2: (1/2)
	room3, err := allocator.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room3", FleetName: fleet1Name})
	require.NoError(t, err)
	require.Equal(t, "group2", room3.Address)
	ev = mustReadChan(t, group2.EventChannel).(*arena.RoomGroupEventRoomAllocated)
	require.Equal(t, "room3", ev.RoomID)

	// group1: (2/2)
	// group2: (2/2)
	room4, err := allocator.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room4", FleetName: fleet1Name})
	require.NoError(t, err)
	require.Equal(t, "group2", room4.Address)
	ev = mustReadChan(t, group2.EventChannel).(*arena.RoomGroupEventRoomAllocated)
	require.Equal(t, "room4", ev.RoomID)

	// all room groups are full
	_, err = allocator.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room5", FleetName: fleet1Name})
	require.ErrorIs(t, err, arena.ErrRoomExhausted)

	// FreeRoom will again free up a slot, which can be allocated
	// group1: (1/2)
	// group2: (2/2)
	err = sdk.FreeRoom(ctx, arena.FreeRoomRequest{Address: "group1", FleetName: fleet1Name})
	require.NoError(t, err)

	room5, err := allocator.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room5", FleetName: fleet1Name})
	require.NoError(t, err)
	require.Equal(t, "group1", room5.Address)
	ev = mustReadChan(t, group1.EventChannel).(*arena.RoomGroupEventRoomAllocated)
	require.Equal(t, "room5", ev.RoomID)

	err = sdk.DeleteRoomGroup(ctx, arena.DeleteRoomGroupRequest{Address: "group1", FleetName: fleet1Name})
	require.NoError(t, err)
	err = sdk.DeleteRoomGroup(ctx, arena.DeleteRoomGroupRequest{Address: "group2", FleetName: fleet1Name})
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

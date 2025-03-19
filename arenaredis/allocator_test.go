package arenaredis

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"

	"github.com/castaneai/arena"
)

const (
	testingKeyPrefix = "arenatest:"
)

func TestAllocator(t *testing.T) {
	fleet1Name := "fleet1"
	ctx := t.Context()
	client := newRedisClientWithMiniRedis(t)
	rg := NewRoomGroupManager(testingKeyPrefix, client)
	allocator := NewRoomAllocator(testingKeyPrefix, client)

	err := rg.AddRoomGroup(ctx, arena.AddRoomGroupRequest{Address: "group1", Capacity: 2, FleetName: fleet1Name})
	require.NoError(t, err)
	room1, err := allocator.AllocateRoom(ctx, "room1", fleet1Name)
	require.NoError(t, err)
	require.Equal(t, "group1", room1.Address)
	room2, err := allocator.AllocateRoom(ctx, "room2", fleet1Name)
	require.NoError(t, err)
	require.Equal(t, "group1", room2.Address)
	_, err = allocator.AllocateRoom(ctx, "room3", fleet1Name)
	require.ErrorIs(t, err, arena.ErrorRoomExhausted)
}

func newRedisClientWithMiniRedis(t *testing.T) rueidis.Client {
	r := miniredis.RunT(t)
	t.Cleanup(func() { r.Close() })
	client, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{r.Addr()}, DisableCache: true})
	if err != nil {
		t.Fatalf("failed to create redis client: %+v", err)
	}
	return client
}

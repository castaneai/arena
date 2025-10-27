package arenaredis

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/castaneai/arena"
)

func TestAddContainerOverwritesExisting(t *testing.T) {
	fleetName := "fleet1"
	ctx := context.Background()
	frontend, backend, _ := newFrontendBackendMetrics(t)

	// Add container with capacity 1
	_, err := backend.AddContainer(ctx, arena.AddContainerRequest{
		ContainerID:     "con1",
		InitialCapacity: 1,
		FleetName:       fleetName,
	})
	require.NoError(t, err)

	// Verify we can allocate 1 room
	room1, err := frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room1", FleetName: fleetName})
	require.NoError(t, err)
	require.Equal(t, "con1", room1.ContainerID)

	// Now container is full (1/1 capacity)
	_, err = frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room2", FleetName: fleetName})
	require.Error(t, err)
	require.True(t, arena.ErrorHasStatus(err, arena.ErrorStatusResourceExhausted))

	// Add the same container again with capacity 2 (should overwrite, not add)
	con2, err := backend.AddContainer(ctx, arena.AddContainerRequest{
		ContainerID:     "con1",
		InitialCapacity: 2,
		FleetName:       fleetName,
	})
	require.NoError(t, err)

	// Verify that room1 is gone - re-allocate room1 should work now
	room1Again, err := frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room1", FleetName: fleetName})
	require.NoError(t, err)
	require.Equal(t, "con1", room1Again.ContainerID)

	// Now we should be able to allocate 1 more room (total 2 capacity)
	room2Again, err := frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room2", FleetName: fleetName})
	require.NoError(t, err)
	require.Equal(t, "con1", room2Again.ContainerID)

	// Now container should be full again (2/2 capacity)
	_, err = frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room3", FleetName: fleetName})
	require.Error(t, err)
	require.True(t, arena.ErrorHasStatus(err, arena.ErrorStatusResourceExhausted))

	// Verify events are received on the second event channel (room1, room2)
	_ = mustReadChan(t, con2.EventChannel)
	_ = mustReadChan(t, con2.EventChannel)

	// Cleanup
	err = backend.DeleteContainer(ctx, arena.DeleteContainerRequest{ContainerID: "con1", FleetName: fleetName})
	require.NoError(t, err)
}

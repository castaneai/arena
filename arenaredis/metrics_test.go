package arenaredis

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/castaneai/arena"
)

func TestMetrics(t *testing.T) {
	ctx := t.Context()
	frontend, backend, metrics := newFrontendBackendMetrics(t)

	fleet1Name := "fleet1"
	fleet1Capacity, err := metrics.GetFleetCapacity(ctx, fleet1Name)
	require.NoError(t, err)
	require.Equal(t, 0, fleet1Capacity)
	fleet1ContainerCount, err := metrics.GetContainerCount(ctx, fleet1Name)
	require.NoError(t, err)
	require.Equal(t, 0, fleet1ContainerCount)

	_, err = backend.AddContainer(ctx, arena.AddContainerRequest{ContainerID: "con1", InitialCapacity: 1, FleetName: fleet1Name})
	require.NoError(t, err)
	_, err = backend.AddContainer(ctx, arena.AddContainerRequest{ContainerID: "con2", InitialCapacity: 2, FleetName: fleet1Name})
	require.NoError(t, err)
	fleet1Capacity, err = metrics.GetFleetCapacity(ctx, fleet1Name)
	require.NoError(t, err)
	require.Equal(t, 3, fleet1Capacity)
	fleet1ContainerCount, err = metrics.GetContainerCount(ctx, fleet1Name)
	require.NoError(t, err)
	require.Equal(t, 2, fleet1ContainerCount)

	resp, err := frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room1", FleetName: fleet1Name})
	require.NoError(t, err)
	fleet1Capacity, err = metrics.GetFleetCapacity(ctx, fleet1Name)
	require.NoError(t, err)
	require.Equal(t, 2, fleet1Capacity)

	err = backend.ReleaseRoom(ctx, arena.ReleaseRoomRequest{ContainerID: resp.ContainerID, FleetName: fleet1Name, RoomID: resp.RoomID})
	require.NoError(t, err)
	fleet1Capacity, err = metrics.GetFleetCapacity(ctx, fleet1Name)
	require.NoError(t, err)
	require.Equal(t, 3, fleet1Capacity)

	_ = backend.DeleteContainer(ctx, arena.DeleteContainerRequest{ContainerID: "con1", FleetName: fleet1Name})
	require.NoError(t, err)
	fleet1Capacity, err = metrics.GetFleetCapacity(ctx, fleet1Name)
	require.NoError(t, err)
	require.Equal(t, 2, fleet1Capacity)
	fleet1ContainerCount, err = metrics.GetContainerCount(ctx, fleet1Name)
	require.NoError(t, err)
	require.Equal(t, 1, fleet1ContainerCount)

	fleet2Name := "fleet2"
	fleet2Capacity, err := metrics.GetFleetCapacity(ctx, fleet2Name)
	require.NoError(t, err)
	require.Equal(t, 0, fleet2Capacity)
	_, err = backend.AddContainer(ctx, arena.AddContainerRequest{ContainerID: "con1", InitialCapacity: 1, FleetName: fleet2Name})
	require.NoError(t, err)
	fleet2Capacity, err = metrics.GetFleetCapacity(ctx, fleet2Name)
	require.NoError(t, err)
	require.Equal(t, 1, fleet2Capacity)
	_, err = frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room2", FleetName: fleet2Name})
	require.NoError(t, err)
	fleet2Capacity, err = metrics.GetFleetCapacity(ctx, fleet2Name)
	require.NoError(t, err)
	require.Equal(t, 0, fleet2Capacity)

	fleet1Capacity, err = metrics.GetFleetCapacity(ctx, fleet1Name)
	require.NoError(t, err)
	require.Equal(t, 2, fleet1Capacity)
}

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

	_, err = backend.AddContainer(ctx, arena.AddContainerRequest{Address: "con1", Capacity: 1, FleetName: fleet1Name})
	require.NoError(t, err)
	_, err = backend.AddContainer(ctx, arena.AddContainerRequest{Address: "con2", Capacity: 2, FleetName: fleet1Name})
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

	err = backend.ReleaseRoom(ctx, arena.ReleaseRoomRequest{Address: resp.Address, ReleaseCapacity: 1, FleetName: fleet1Name})
	require.NoError(t, err)
	fleet1Capacity, err = metrics.GetFleetCapacity(ctx, fleet1Name)
	require.NoError(t, err)
	require.Equal(t, 3, fleet1Capacity)

	_ = backend.DeleteContainer(ctx, arena.DeleteContainerRequest{Address: "con1", FleetName: fleet1Name})
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
	_, err = backend.AddContainer(ctx, arena.AddContainerRequest{Address: "con1", Capacity: 1, FleetName: fleet2Name})
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

package arenaredis

import (
	"context"
	"testing"
	"time"

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

// A container that retries AddContainer with the same registration ID is still the same
// incarnation, so the rooms allocated to it must survive the retry.
func TestAddContainerRetryKeepsAllocatedRooms(t *testing.T) {
	fleetName := "fleet1"
	ctx := t.Context()
	frontend, backend, _ := newFrontendBackendMetrics(t)

	registrationID := "registration-1"
	_, err := backend.AddContainer(ctx, arena.AddContainerRequest{
		ContainerID:     "con1",
		InitialCapacity: 1,
		FleetName:       fleetName,
		RegistrationID:  registrationID,
	})
	require.NoError(t, err)

	room1, err := frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room1", FleetName: fleetName})
	require.NoError(t, err)
	require.Equal(t, "con1", room1.ContainerID)

	// The container did not see the response of the first call (e.g. an RPC timeout) and retries.
	con1Retried, err := backend.AddContainer(ctx, arena.AddContainerRequest{
		ContainerID:     "con1",
		InitialCapacity: 1,
		FleetName:       fleetName,
		RegistrationID:  registrationID,
	})
	require.NoError(t, err)

	// room1 still occupies the only slot, so no other room can be allocated to con1.
	_, err = frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room2", FleetName: fleetName})
	require.True(t, arena.ErrorHasStatus(err, arena.ErrorStatusResourceExhausted))

	// room1 is still reachable, and its events arrive on the channel of the retried call.
	err = frontend.NotifyToRoom(ctx, arena.NotifyToRoomRequest{RoomID: "room1", FleetName: fleetName, Body: []byte("hello_room1")})
	require.NoError(t, err)
	ev := mustReadChan(t, con1Retried.EventChannel).(*arena.NotifyToRoomEvent)
	require.Equal(t, "room1", ev.RoomID)
	require.Equal(t, "hello_room1", string(ev.Body))

	// Releasing room1 frees the slot as usual.
	err = backend.ReleaseRoom(ctx, arena.ReleaseRoomRequest{ContainerID: "con1", FleetName: fleetName, RoomID: "room1"})
	require.NoError(t, err)
	room2, err := frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room2", FleetName: fleetName})
	require.NoError(t, err)
	require.Equal(t, "con1", room2.ContainerID)
}

// A container ID reused by a new incarnation (a restarted process or Pod) cannot serve the rooms of
// the previous one, so those room mappings are removed and the capacity is reset.
func TestAddContainerNewRegistrationClearsRooms(t *testing.T) {
	fleetName := "fleet1"
	ctx := t.Context()
	frontend, backend, _ := newFrontendBackendMetrics(t)

	_, err := backend.AddContainer(ctx, arena.AddContainerRequest{
		ContainerID:     "con1",
		InitialCapacity: 1,
		FleetName:       fleetName,
		RegistrationID:  "registration-1",
	})
	require.NoError(t, err)

	room1, err := frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room1", FleetName: fleetName})
	require.NoError(t, err)
	require.Equal(t, "con1", room1.ContainerID)

	_, err = backend.AddContainer(ctx, arena.AddContainerRequest{
		ContainerID:     "con1",
		InitialCapacity: 1,
		FleetName:       fleetName,
		RegistrationID:  "registration-2",
	})
	require.NoError(t, err)

	err = frontend.NotifyToRoom(ctx, arena.NotifyToRoomRequest{RoomID: "room1", FleetName: fleetName, Body: []byte("hello_room1")})
	require.True(t, arena.ErrorHasStatus(err, arena.ErrorStatusNotFound))

	room2, err := frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room2", FleetName: fleetName})
	require.NoError(t, err)
	require.Equal(t, "con1", room2.ContainerID)
}

// Registering with no capacity keeps the allocated rooms even without a registration ID, because a
// container that reports no capacity cannot have been restarted into an empty state.
func TestAddContainerZeroCapacityKeepsAllocatedRooms(t *testing.T) {
	fleetName := "fleet1"
	ctx := t.Context()
	frontend, backend, _ := newFrontendBackendMetrics(t)

	_, err := backend.AddContainer(ctx, arena.AddContainerRequest{
		ContainerID:     "con1",
		InitialCapacity: 1,
		FleetName:       fleetName,
	})
	require.NoError(t, err)

	room1, err := frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room1", FleetName: fleetName})
	require.NoError(t, err)
	require.Equal(t, "con1", room1.ContainerID)

	_, err = backend.AddContainer(ctx, arena.AddContainerRequest{
		ContainerID:     "con1",
		InitialCapacity: 0,
		FleetName:       fleetName,
	})
	require.NoError(t, err)

	err = frontend.NotifyToRoom(ctx, arena.NotifyToRoomRequest{RoomID: "room1", FleetName: fleetName, Body: []byte("hello_room1")})
	require.NoError(t, err)

	_, err = frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room2", FleetName: fleetName})
	require.True(t, arena.ErrorHasStatus(err, arena.ErrorStatusResourceExhausted))
}

// The registration ID must stay alive as long as the container sends heartbeats, otherwise a later
// retry looks like a new incarnation and drops the rooms in use.
func TestAddContainerRetryAfterHeartbeatKeepsAllocatedRooms(t *testing.T) {
	fleetName := "fleet1"
	ctx := t.Context()
	frontend, backend, _ := newFrontendBackendMetrics(t)

	ttl := 3 * time.Second
	registrationID := "registration-1"
	_, err := backend.AddContainer(ctx, arena.AddContainerRequest{
		ContainerID:     "con1",
		InitialCapacity: 1,
		FleetName:       fleetName,
		HeartbeatTTL:    ttl,
		RegistrationID:  registrationID,
	})
	require.NoError(t, err)

	room1, err := frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room1", FleetName: fleetName})
	require.NoError(t, err)
	require.Equal(t, "con1", room1.ContainerID)

	// Send a heartbeat before the TTL expires, then let the original TTL pass.
	time.Sleep(ttl - time.Second)
	require.NoError(t, backend.SendHeartbeat(ctx, arena.SendHeartbeatRequest{ContainerID: "con1", FleetName: fleetName}))
	time.Sleep(2 * time.Second)

	_, err = backend.AddContainer(ctx, arena.AddContainerRequest{
		ContainerID:     "con1",
		InitialCapacity: 1,
		FleetName:       fleetName,
		HeartbeatTTL:    ttl,
		RegistrationID:  registrationID,
	})
	require.NoError(t, err)

	err = frontend.NotifyToRoom(ctx, arena.NotifyToRoomRequest{RoomID: "room1", FleetName: fleetName, Body: []byte("hello_room1")})
	require.NoError(t, err)
	_, err = frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room2", FleetName: fleetName})
	require.True(t, arena.ErrorHasStatus(err, arena.ErrorStatusResourceExhausted))
}

// Keeping the rooms of a re-registered container cannot block its capacity forever: the registration
// ID lives on the heartbeat TTL, so a container that stops reporting liveness is registered again as
// a new incarnation and its rooms are cleared.
func TestAddContainerRetryAfterHeartbeatExpiredClearsRooms(t *testing.T) {
	fleetName := "fleet1"
	ctx := t.Context()
	frontend, backend, _ := newFrontendBackendMetrics(t)

	ttl := 2 * time.Second
	registrationID := "registration-1"
	_, err := backend.AddContainer(ctx, arena.AddContainerRequest{
		ContainerID:     "con1",
		InitialCapacity: 1,
		FleetName:       fleetName,
		HeartbeatTTL:    ttl,
		RegistrationID:  registrationID,
	})
	require.NoError(t, err)

	room1, err := frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room1", FleetName: fleetName})
	require.NoError(t, err)
	require.Equal(t, "con1", room1.ContainerID)

	// Let the heartbeat lapse, which expires the registration ID as well.
	time.Sleep(ttl + time.Second)

	_, err = backend.AddContainer(ctx, arena.AddContainerRequest{
		ContainerID:     "con1",
		InitialCapacity: 1,
		FleetName:       fleetName,
		HeartbeatTTL:    ttl,
		RegistrationID:  registrationID,
	})
	require.NoError(t, err)

	err = frontend.NotifyToRoom(ctx, arena.NotifyToRoomRequest{RoomID: "room1", FleetName: fleetName, Body: []byte("hello_room1")})
	require.True(t, arena.ErrorHasStatus(err, arena.ErrorStatusNotFound))

	room2, err := frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room2", FleetName: fleetName})
	require.NoError(t, err)
	require.Equal(t, "con1", room2.ContainerID)
}

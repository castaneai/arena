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

// A container whose events nobody reads must not stall the connection it subscribes on.
//
// The pub/sub hook runs on the only read loop of that connection, so a send that blocks there stops
// every reply on it, including the UNSUBSCRIBE that releasing the dedicated client waits for with a
// context that cannot be cancelled. Releasing happens while the fleet lock is held, so one stalled
// container used to take AddContainer and DeleteContainer of its whole fleet down with it, for the
// rest of the process lifetime.
func TestAddContainerNotBlockedByUnreadContainerEvents(t *testing.T) {
	fleetName := "fleet1"
	ctx := t.Context()
	keyPrefix := fmt.Sprintf("arenaredis_test_%s", uuid.New().String())
	publisher := newRedisClient(t)
	backend := NewBackend(keyPrefix, newRedisClient(t))

	req := arena.AddContainerRequest{ContainerID: "con1", InitialCapacity: 1, FleetName: fleetName}
	_, err := backend.AddContainer(ctx, req)
	require.NoError(t, err)

	channel := redisPubSubChannelContainer(keyPrefix, fleetName, "con1")
	// An event arena cannot decode used to end the goroutine that drains the subscription.
	mustPublish(t, publisher, channel, "not_a_valid_event")
	time.Sleep(200 * time.Millisecond)
	// With nobody draining, this send blocks inside the hook and stalls the whole connection.
	mustPublish(t, publisher, channel, "not_a_valid_event_either")
	time.Sleep(200 * time.Millisecond)

	// Re-registering releases the previous container, which is where the stall used to spread.
	done := make(chan error, 1)
	go func() {
		_, err := backend.AddContainer(ctx, req)
		done <- err
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("AddContainer did not return: the fleet lock is likely held by a stalled release")
	}

	// The fleet is still usable for other containers as well.
	_, err = backend.AddContainer(ctx, arena.AddContainerRequest{ContainerID: "con2", InitialCapacity: 1, FleetName: fleetName})
	require.NoError(t, err)
	require.NoError(t, backend.DeleteContainer(ctx, arena.DeleteContainerRequest{ContainerID: "con2", FleetName: fleetName}))
}

// The events of a container that has stopped reading are dropped, not left to block the connection.
func TestContainerKeepsReceivingAfterAnUndecodableEvent(t *testing.T) {
	fleetName := "fleet1"
	ctx := t.Context()
	keyPrefix := fmt.Sprintf("arenaredis_test_%s", uuid.New().String())
	publisher := newRedisClient(t)
	backend := NewBackend(keyPrefix, newRedisClient(t))
	frontend := NewFrontend(keyPrefix, newRedisClient(t))

	con1, err := backend.AddContainer(ctx, arena.AddContainerRequest{ContainerID: "con1", InitialCapacity: 1, FleetName: fleetName})
	require.NoError(t, err)

	channel := redisPubSubChannelContainer(keyPrefix, fleetName, "con1")
	mustPublish(t, publisher, channel, "not_a_valid_event")
	time.Sleep(200 * time.Millisecond)

	// A single event arena cannot decode must not cost the container the ones that follow.
	_, err = frontend.AllocateRoom(ctx, arena.AllocateRoomRequest{RoomID: "room1", FleetName: fleetName})
	require.NoError(t, err)
	ev := mustReadChan(t, con1.EventChannel).(*arena.AllocationEvent)
	require.Equal(t, "room1", ev.RoomID)
}

func mustPublish(t *testing.T, client rueidis.Client, channel, message string) {
	t.Helper()
	require.NoError(t, client.Do(t.Context(), client.B().Publish().Channel(channel).Message(message).Build()).Error())
}

func newRedisClient(t *testing.T) rueidis.Client {
	t.Helper()
	client, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{localRedisAddr}, DisableCache: true})
	if err != nil {
		t.Fatalf("failed to create redis client: %+v", err)
	}
	checkRedisConnection(t, client)
	return client
}

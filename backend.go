package arena

import (
	"context"
	"time"
)

const (
	DefaultHeartbeatTTL = 30 * time.Second
)

type Backend interface {
	// AddContainer adds a container to arena.
	AddContainer(ctx context.Context, req AddContainerRequest) (*AddContainerResponse, error)

	// DeleteContainer removes a container from arena.
	DeleteContainer(ctx context.Context, req DeleteContainerRequest) error

	// ReleaseRoom releases a room and makes it available for allocation.
	ReleaseRoom(ctx context.Context, req ReleaseRoomRequest) error

	// SendHeartbeat sends a heartbeat to keep the container alive.
	SendHeartbeat(ctx context.Context, req SendHeartbeatRequest) error
}

type AddContainerRequest struct {
	ContainerID     string
	FleetName       string
	InitialCapacity int
	HeartbeatTTL    time.Duration // TTL for heartbeat, uses DefaultHeartbeatTTL if 0

	// RegistrationID identifies a single container incarnation, such as one process or one Pod
	// lifetime. Generate it once when the container starts (e.g. a UUID) and send the same value on
	// every AddContainer call made by that container, including retries.
	//
	// It must not be derived from anything that survives a restart, the container ID or the Pod name
	// in particular: a restarted container that reports the RegistrationID of its predecessor keeps
	// the rooms of a session it no longer serves, and that capacity is only freed once its heartbeat
	// lapses. Rooms a container no longer serves are released with Backend.ReleaseRoom.
	//
	// When AddContainer receives the RegistrationID that arena already holds for the same
	// ContainerID, it treats the call as a re-registration of the running container: the rooms
	// already allocated to it are kept and its capacity stays consistent with them. Only the event
	// subscription is re-established. Any other RegistrationID, including an empty one, means a new
	// incarnation, so the rooms left behind by the previous one are removed.
	//
	// Rooms are never removed from a container registering with an InitialCapacity of 0, whatever
	// its RegistrationID: reporting no capacity states that the slots are taken.
	//
	// A container that retries AddContainer (e.g. after an RPC timeout) should always set this
	// field: otherwise a retry frees capacity that is in fact occupied, and the room mapping of the
	// session running there is lost.
	RegistrationID string
}

type AddContainerResponse struct {
	EventChannel <-chan ToContainerEvent
}

type ToContainerEvent interface {
	toContainerEvent()
}

type AllocationEvent struct {
	RoomID          string
	RoomInitialData []byte
}

func (e *AllocationEvent) toContainerEvent() {}

type NotifyToRoomEvent struct {
	RoomID string
	Body   []byte
}

func (e *NotifyToRoomEvent) toContainerEvent() {}

type DeleteContainerRequest struct {
	ContainerID string
	FleetName   string
}

type ReleaseRoomRequest struct {
	ContainerID string
	FleetName   string
	RoomID      string
}

type SendHeartbeatRequest struct {
	ContainerID string
	FleetName   string
}

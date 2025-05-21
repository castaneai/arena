# Arena

Arena manages room allocations for multiplayer games.

A **Room** is the place where a single game session starts.
The process of starting a multiplayer game (e.g. Matchmaker) calls `Frontend.AllocateRoom` and returns the address to the player.

A **Container** is a place to store multiple rooms, usually an OS process or a Kubernetes Pod.
Containers provide their own address and capacity at startup with `Backend.AddContainer`.
and also detect new room allocations via `AddContainerResponse.EventChannel`.

A **Fleet** is a group of Containers, and `Frontend.AllocateRoom` allows you to specify to which Fleet a Room is assigned.
You may have multiple Fleets depending on the environment and game type.

Each time a room is allocated, the capacity of the Container is decremented by 1.
When it reaches 0, the Container is full and cannot be allocated there.
However, when a room is freed by `Backend.ReleaseRoom`, the capacity is increased and the room can be allocated again.

Note that capacity here is the number of rooms, not the number of players.

```mermaid
sequenceDiagram
    participant Player
    participant Matchmaker
    participant Arena
    participant Container

    Container ->> Arena: Backend.AddContainer
    loop Container is alive
        Player ->> Matchmaker: Request Matchmaking
        Matchmaker ->> Arena: Frontend.AllocateRoom()
        activate Arena
        Arena ->> Container: Notify Allocation
        Arena ->> Matchmaker: Container Address
        deactivate Arena
        Matchmaker ->> Player: Container Address
        Note over Container: Container capacity -1

        Player ->> Container: Join Room
        Note over Container: Game session occurs...
        Player ->> Container: Leave Room
        Container ->> Arena: Backend.SetRoomResult()
        Container ->> Arena: Backend.ReleaseRoom()
        Note over Container: Container capacity +1
    end
    Note over Container: Shutdown Container
    Container ->> Arena: Backend.DeleteContainer
```

## Usage

Arena uses Redis as its backend, with [rueidis](https://github.com/redis/rueidis).
You can create the respective interfaces with `arenaredis.NewFrontend` and `arenaredis.NewBackend`.

## Extensibility

Arena is currently built on top of Go and Redis,
but could be implemented in other languages or other pub/sub solutions, as Arena is essentially a room management interface.

## License

MIT

package arenaredis

import (
	"fmt"
)

func redisKeyFleetCapacities(prefix string) string {
	return fmt.Sprintf("%sfleets_capacities", prefix)
}

func redisKeyAvailableContainersIndex(prefix, fleetName string) string {
	return fmt.Sprintf("%s%s:container_index", prefix, fleetName)
}

func redisKeyRoomToContainer(prefix, fleetName, roomID string) string {
	return fmt.Sprintf("%s%s:room_container:%s", prefix, fleetName, roomID)
}

func redisKeyContainerToRoomsPrefix(prefix, fleetName string) string {
	return fmt.Sprintf("%s%s:container_rooms:", prefix, fleetName)
}

func redisKeyContainerToRooms(prefix, fleetName, containerID string) string {
	return fmt.Sprintf("%s%s", redisKeyContainerToRoomsPrefix(prefix, fleetName), containerID)
}

func redisPubSubChannelContainerPrefix(prefix, fleetName string) string {
	return fmt.Sprintf("%s%s:container_channel:", prefix, fleetName)
}

func redisPubSubChannelContainer(prefix, fleetName, containerID string) string {
	return fmt.Sprintf("%s%s", redisPubSubChannelContainerPrefix(prefix, fleetName), containerID)
}

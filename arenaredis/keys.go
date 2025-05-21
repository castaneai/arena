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

func redisKeyRoomResult(prefix, roomID string) string {
	return fmt.Sprintf("%s%s:room_result", prefix, roomID)
}

func redisPubSubChannelContainerPrefix(prefix, fleetName string) string {
	return fmt.Sprintf("%s%s:channel:", prefix, fleetName)
}

func redisPubSubChannelContainer(prefix, fleetName, containerID string) string {
	return fmt.Sprintf("%s%s:channel:%s", prefix, fleetName, containerID)
}

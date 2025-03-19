package arenaredis

import "fmt"

func redisKeyAvailableRoomGroups(prefix, fleetName string) string {
	return fmt.Sprintf("%s%s:available_room_groups", prefix, fleetName)
}

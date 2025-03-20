package arenaredis

import "fmt"

const (
	redisStreamFieldNameEventName       = "RoomGroupEventName"
	redisStreamFieldNameRoomID          = "RoomID"
	redisStreamFieldNameRoomInitialData = "RoomInitialData"
)

func redisKeyAvailableRoomGroups(prefix, fleetName string) string {
	return fmt.Sprintf("%s%s:available_room_groups", prefix, fleetName)
}

func redisKeyRoomGroupEventStream(prefix, fleetName, address string) string {
	return fmt.Sprintf("%s%s:room_group_events:%s", prefix, fleetName, address)
}

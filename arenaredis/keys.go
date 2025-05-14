package arenaredis

import (
	"fmt"
)

const (
	streamFieldRequestID       = "RequestID"
	streamFieldRoomID          = "RoomID"
	streamFieldRoomInitialData = "RoomInitialData"
)

func redisKeyFleetStream(prefix, fleetName string) string {
	return fmt.Sprintf("%s%s", prefix, fleetName)
}

func redisKeyRoomResult(prefix, roomID string) string {
	return fmt.Sprintf("%s%s:room_result", prefix, roomID)
}

func redisPubSubKeyAllocationAck(prefix, fleetName, requestID string) string {
	return fmt.Sprintf("%s%s:allocation_ack:%s", prefix, fleetName, requestID)
}

func redisFleetConsumerGroup(prefix, fleetName string) string {
	return fmt.Sprintf("%s%s", prefix, fleetName)
}

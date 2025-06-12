package arenaredis

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/redis/rueidis"

	"github.com/castaneai/arena"
)

const (
	toContainerEventNameAllocationEvent   = "AllocationEvent"
	toContainerEventNameNotifyToRoomEvent = "NotifyToRoomEvent"
)

type allocationEventJSON struct {
	RoomID          string `json:"room_id"`
	RoomInitialData string `json:"room_initial_data,omitempty"`
}

type notifyToRoomEventJSON struct {
	RoomID string `json:"room_id"`
	Body   string `json:"body"`
}

func encodeAllocationEvent(roomID string, roomInitialData []byte) (string, error) {
	j := allocationEventJSON{
		RoomID:          roomID,
		RoomInitialData: base64.StdEncoding.EncodeToString(roomInitialData),
	}
	bytes, err := json.Marshal(j)
	if err != nil {
		return "", fmt.Errorf("failed to encode AllocationEvent: %w", err)
	}
	return toContainerEventNameAllocationEvent + ":" + rueidis.BinaryString(bytes), nil
}

func encodeNotifyToRoomEvent(roomID string, body []byte) (string, error) {
	j := notifyToRoomEventJSON{
		RoomID: roomID,
		Body:   base64.StdEncoding.EncodeToString(body),
	}
	bytes, err := json.Marshal(j)
	if err != nil {
		return "", fmt.Errorf("failed to encode NotifyToRoomEvent: %w", err)
	}
	return toContainerEventNameNotifyToRoomEvent + ":" + rueidis.BinaryString(bytes), nil
}

func decodeToContainerEvent(data string) (arena.ToContainerEvent, error) {
	parts := strings.SplitN(data, ":", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("failed to decode toContainer event: invalid format, expected 'event_name:data'")
	}
	eventName := parts[0]
	body := parts[1]

	switch eventName {
	case toContainerEventNameAllocationEvent:
		var j allocationEventJSON
		if err := json.Unmarshal([]byte(body), &j); err != nil {
			return nil, fmt.Errorf("failed to decode allocation event: %w", err)
		}
		if j.RoomID == "" {
			return nil, fmt.Errorf("failed to decode allocation event: missing room_id")
		}
		result := &arena.AllocationEvent{
			RoomID: j.RoomID,
		}
		if j.RoomInitialData != "" {
			roomInitialData, err := base64.StdEncoding.DecodeString(j.RoomInitialData)
			if err != nil {
				return nil, fmt.Errorf("failed to decode room initial data: %w", err)
			}
			result.RoomInitialData = roomInitialData
		}
		return result, nil
	case toContainerEventNameNotifyToRoomEvent:
		var j notifyToRoomEventJSON
		if err := json.Unmarshal([]byte(body), &j); err != nil {
			return nil, fmt.Errorf("failed to decode NotifyToRoomEvent: %w", err)
		}
		if j.RoomID == "" {
			return nil, fmt.Errorf("failed to decode NotifyToRoomEvent: missing room_id")
		}
		result := &arena.NotifyToRoomEvent{
			RoomID: j.RoomID,
		}
		b, err := base64.StdEncoding.DecodeString(j.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to decode NotifyToRoomEvent body: %w", err)
		}
		result.Body = b
		return result, nil
	default:
		return nil, fmt.Errorf("failed to decode toContainer event: unknown event name '%s'", eventName)
	}
}

func encodeHeartbeatTTLValue(ttl time.Duration) string {
	return fmt.Sprintf("alive:%d", int(ttl.Seconds()))
}

func decodeHeartbeatTTLValue(value string) (time.Duration, error) {
	var ttlSeconds int
	if _, err := fmt.Sscanf(value, "alive:%d", &ttlSeconds); err != nil {
		return 0, fmt.Errorf("failed to parse heartbeat TTL value: %w", err)
	}
	return time.Duration(ttlSeconds) * time.Second, nil
}

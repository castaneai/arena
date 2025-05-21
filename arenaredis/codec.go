package arenaredis

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/redis/rueidis"

	"github.com/castaneai/arena"
)

type allocationEventJSON struct {
	RoomID          string `json:"room_id"`
	RoomInitialData string `json:"room_initial_data,omitempty"`
}

func encodeRoomAllocationEvent(ev arena.AllocationEvent) (string, error) {
	j := allocationEventJSON{
		RoomID:          ev.RoomID,
		RoomInitialData: base64.StdEncoding.EncodeToString(ev.RoomInitialData),
	}
	data, err := json.Marshal(j)
	if err != nil {
		return "", fmt.Errorf("failed to encode allocation event: %w", err)
	}
	return rueidis.BinaryString(data), nil
}

func decodeRoomAllocationEvent(data string) (*arena.AllocationEvent, error) {
	var j allocationEventJSON
	if err := json.Unmarshal([]byte(data), &j); err != nil {
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
}

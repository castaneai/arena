package arenaredis

import (
	"fmt"
	"strings"
)

type roomAllocationAck struct {
	roomID  string
	address string
}

func deserializeRoomAllocationAck(data string) (*roomAllocationAck, error) {
	parts := strings.Split(data, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid allocation ack format: %s", data)
	}
	return &roomAllocationAck{
		roomID:  parts[0],
		address: parts[1],
	}, nil
}

func (a *roomAllocationAck) Serialize() string {
	return fmt.Sprintf("%s:%s", a.roomID, a.address)
}

package arenaredis

import (
	"context"

	"github.com/redis/rueidis"

	"github.com/castaneai/arena"
)

type redisRoomGroupManager struct {
	keyPrefix string
	client    rueidis.Client
}

func NewRoomGroupManager(keyPrefix string, client rueidis.Client) arena.RoomGroupManager {
	return &redisRoomGroupManager{keyPrefix: keyPrefix, client: client}
}

func (m *redisRoomGroupManager) AddRoomGroup(ctx context.Context, req arena.AddRoomGroupRequest) error {
	key := redisKeyAvailableRoomGroups(m.keyPrefix, req.FleetName)
	cmd := m.client.B().Zadd().Key(key).ScoreMember().ScoreMember(float64(req.Capacity), req.Address).Build()
	if err := m.client.Do(ctx, cmd).Error(); err != nil {
		return err
	}
	return nil
}

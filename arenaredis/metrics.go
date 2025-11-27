package arenaredis

import (
	"context"
	"fmt"

	"github.com/redis/rueidis"
)

type ContainerCapacity struct {
	ContainerID string
	Capacity    int
}

type Metrics struct {
	keyPrefix string
	client    rueidis.Client
}

func NewMetrics(keyPrefix string, client rueidis.Client) *Metrics {
	return &Metrics{keyPrefix: keyPrefix, client: client}
}

func (m *Metrics) GetContainerCount(ctx context.Context, fleetName string) (int, error) {
	key := redisKeyAvailableContainersIndex(m.keyPrefix, fleetName)
	cmd := m.client.B().Zcard().Key(key).Build()
	res := m.client.Do(ctx, cmd)
	if err := res.Error(); err != nil {
		return 0, fmt.Errorf("failed to zcard available containers index: %w", err)
	}
	count, err := res.AsInt64()
	if err != nil {
		return 0, fmt.Errorf("failed to parse containers count as int64: %w", err)
	}
	return int(count), nil
}

func (m *Metrics) GetContainers(ctx context.Context, fleetName string) ([]ContainerCapacity, error) {
	key := redisKeyAvailableContainersIndex(m.keyPrefix, fleetName)

	// Get all containers (including capacity: 0) to check for expired ones
	cmd := m.client.B().Zrange().Key(key).Min("-inf").Max("+inf").Byscore().Withscores().Build()
	res := m.client.Do(ctx, cmd)
	if err := res.Error(); err != nil {
		return nil, fmt.Errorf("failed to zrange available containers index: %w", err)
	}

	scores, err := res.AsZScores()
	if err != nil {
		return nil, fmt.Errorf("failed to parse zrange result: %w", err)
	}

	var containers []ContainerCapacity
	var expiredContainerIDs []string
	for _, score := range scores {
		containerID := score.Member
		capacity := int(score.Score)

		// Check if container heartbeat has expired
		expired, err := isContainerExpired(ctx, m.client, m.keyPrefix, fleetName, containerID)
		if err != nil || expired {
			expiredContainerIDs = append(expiredContainerIDs, containerID)
			continue // Skip expired containers
		}

		// Only include containers with capacity >= 1 in the result
		if capacity >= 1 {
			containers = append(containers, ContainerCapacity{
				ContainerID: containerID,
				Capacity:    capacity,
			})
		}
	}

	// Remove expired containers from the index
	if len(expiredContainerIDs) > 0 {
		zremCmd := m.client.B().Zrem().Key(key).Member(expiredContainerIDs...).Build()
		if err := m.client.Do(ctx, zremCmd).Error(); err != nil {
			return nil, fmt.Errorf("failed to remove expired containers from index: %w", err)
		}
	}

	return containers, nil
}

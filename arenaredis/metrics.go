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
	var containers []ContainerCapacity
	var cursor uint64

	for {
		cmd := m.client.B().Zscan().Key(key).Cursor(cursor).Match("*").Count(100).Build()
		res := m.client.Do(ctx, cmd)
		if err := res.Error(); err != nil {
			return nil, fmt.Errorf("failed to zscan available containers index: %w", err)
		}

		scanResult, err := res.AsScanEntry()
		if err != nil {
			return nil, fmt.Errorf("failed to parse zscan result: %w", err)
		}

		// Process the returned elements (they come as [member, score, member, score, ...])
		for i := 0; i < len(scanResult.Elements); i += 2 {
			if i+1 >= len(scanResult.Elements) {
				break
			}

			containerID := scanResult.Elements[i]
			scoreStr := scanResult.Elements[i+1]

			// Parse score directly as int
			var capacity int
			if _, err := fmt.Sscanf(scoreStr, "%d", &capacity); err != nil {
				continue // Skip invalid scores
			}

			// Only include containers with capacity > 0
			if capacity <= 0 {
				continue
			}

			// Check if container heartbeat has expired
			expired, err := isContainerExpired(ctx, m.client, m.keyPrefix, fleetName, containerID)
			if err != nil || expired {
				continue // Skip expired containers
			}

			containers = append(containers, ContainerCapacity{
				ContainerID: containerID,
				Capacity:    capacity,
			})
		}

		cursor = scanResult.Cursor
		if cursor == 0 {
			break // Scan completed
		}
	}

	return containers, nil
}

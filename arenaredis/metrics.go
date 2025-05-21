package arenaredis

import (
	"context"
	"fmt"

	"github.com/redis/rueidis"
)

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

func (m *Metrics) GetFleetCapacity(ctx context.Context, fleetName string) (int, error) {
	key := redisKeyFleetCapacities(m.keyPrefix)
	cmd := m.client.B().Zscore().Key(key).Member(fleetName).Build()
	res := m.client.Do(ctx, cmd)
	if err := res.Error(); err != nil {
		if rueidis.IsRedisNil(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to zscore fleet capacities: %w", err)
	}
	capacity, err := res.AsInt64()
	if err != nil {
		return 0, fmt.Errorf("failed to parse fleet capacity as int64: %w", err)
	}
	return int(capacity), nil
}

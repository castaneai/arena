package arenaredis

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/rueidis"

	"github.com/castaneai/arena"
)

const (
	defaultAllocationChannelBufferSize = 1024
)

// redisDoer is an interface that abstracts the Do and B methods needed for Redis operations.
// This allows isContainerExpired to work with both rueidis.Client and rueidis.DedicatedClient.
type redisDoer interface {
	Do(ctx context.Context, cmd rueidis.Completed) rueidis.RedisResult
	B() rueidis.Builder
}

type container struct {
	containerID string
	fleetName   string
	// subscriber carries the subscription only. Its read loop delivers the pub/sub messages, so
	// any command sent on it waits behind them, and a reply cannot arrive while the listener of
	// this container is busy elsewhere.
	subscriber rueidis.DedicatedClient
	// client is the shared connection pool, used for the liveness checks of this container.
	client    rueidis.Client
	keyPrefix string
	stopCtx   context.Context
	stopFunc  context.CancelFunc
}

func newContainer(client rueidis.Client, keyPrefix string, req arena.AddContainerRequest) *container {
	stopCtx, stopFunc := context.WithCancel(context.Background())
	dc, releaseDedicatedClient := client.Dedicate()
	return &container{
		containerID: req.ContainerID,
		fleetName:   req.FleetName,
		subscriber:  dc,
		client:      client,
		keyPrefix:   keyPrefix,
		stopCtx:     stopCtx,
		stopFunc: func() {
			releaseDedicatedClient()
			stopFunc()
		},
	}
}

func (c *container) stop() {
	c.stopFunc()
}

// start receives events occurring in a specific Room in realtime.
func (c *container) start() (<-chan arena.ToContainerEvent, error) {
	ch := make(chan arena.ToContainerEvent, defaultAllocationChannelBufferSize)
	channel := redisPubSubChannelContainer(c.keyPrefix, c.fleetName, c.containerID)
	received, err := subscribe(c.stopCtx, c.subscriber, channel)
	if err != nil {
		return nil, err
	}
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-c.stopCtx.Done():
				return
			case msg := <-received:
				ev, err := decodeToContainerEvent(msg)
				if err != nil {
					// One message arena cannot read is not a reason to take the container out of
					// the fleet: giving up here would leave every later event undelivered until
					// the container registers again.
					slog.Error(fmt.Sprintf("failed to decode allocation event, skipping it: %v", err))
					continue
				}
				select {
				case ch <- ev:
				default:
					slog.Error(fmt.Sprintf("toContainer event received but channel is full: %+v", ev))
				}
			case <-ticker.C:
				// Check if container was explicitly deleted (removed from index)
				deleted, err := c.isDeleted(c.stopCtx)
				if err != nil {
					slog.WarnContext(c.stopCtx, fmt.Sprintf("failed to check if container is deleted: %+v", err), "error", err)
					continue
				}
				if deleted {
					// Container was explicitly deleted by another backend, stop silently
					c.stop()
					return
				}
				expired, err := c.isExpired(c.stopCtx)
				if err != nil {
					slog.WarnContext(c.stopCtx, fmt.Sprintf("failed to check if container is expired: %+v", err), "error", err)
					continue
				}
				if expired {
					slog.InfoContext(c.stopCtx, fmt.Sprintf("container '%s' heartbeat expired, stopping pubsub listener", c.containerID))
					c.stop()
					return
				}
			}
		}
	}()
	return ch, nil
}

func (c *container) isExpired(ctx context.Context) (bool, error) {
	return isContainerExpired(ctx, c.client, c.keyPrefix, c.fleetName, c.containerID)
}

// isDeleted checks if the container was explicitly deleted (removed from available containers index).
func (c *container) isDeleted(ctx context.Context) (bool, error) {
	key := redisKeyAvailableContainersIndex(c.keyPrefix, c.fleetName)
	cmd := c.client.B().Zscore().Key(key).Member(c.containerID).Build()
	res := c.client.Do(ctx, cmd)
	if err := res.Error(); err != nil {
		if rueidis.IsRedisNil(err) {
			// Container not in index means it was deleted
			return true, nil
		}
		return false, fmt.Errorf("failed to check if container exists in index: %w", err)
	}
	// Container exists in index
	return false, nil
}

// isContainerExpired checks if a container's heartbeat has expired by checking if the heartbeat key exists in Redis.
// This is a common function that can be used by both container and metrics.
func isContainerExpired(ctx context.Context, client redisDoer, keyPrefix, fleetName, containerID string) (bool, error) {
	key := redisKeyContainerHeartbeat(keyPrefix, fleetName, containerID)
	cmd := client.B().Exists().Key(key).Build()
	res := client.Do(ctx, cmd)
	if err := res.Error(); err != nil {
		if rueidis.IsRedisNil(err) {
			return true, nil
		}
		return false, fmt.Errorf("failed to check if container heartbeat exists: %w", err)
	}
	existsCount, err := res.AsInt64()
	if err != nil {
		return false, fmt.Errorf("failed to parse exists count as int64: %w", err)
	}
	return existsCount == 0, nil
}

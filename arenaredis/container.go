package arenaredis

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/redis/rueidis"

	"github.com/castaneai/arena"
)

const (
	defaultAllocationChannelBufferSize = 1024
)

type container struct {
	containerID string
	fleetName   string
	client      rueidis.Client
	keyPrefix   string
	stopCtx     context.Context
	stopFunc    context.CancelFunc
}

func newContainer(client rueidis.Client, keyPrefix string, req arena.AddContainerRequest) *container {
	stopCtx, stopFunc := context.WithCancel(context.Background())
	return &container{
		containerID: req.ContainerID,
		fleetName:   req.FleetName,
		client:      client,
		keyPrefix:   keyPrefix,
		stopCtx:     stopCtx,
		stopFunc:    stopFunc,
	}
}

func (c *container) stop() {
	c.stopFunc()
}

// start uses Redis Streams to receive events occurring in a specific Room in realtime.
func (c *container) start() (<-chan arena.AllocationEvent, error) {
	ch := make(chan arena.AllocationEvent, defaultAllocationChannelBufferSize)

	dc, stopDedicatedClient := c.client.Dedicate()
	subscribed := make(chan struct{})
	pubsubClosed := dc.SetPubSubHooks(rueidis.PubSubHooks{
		OnMessage: func(msg rueidis.PubSubMessage) {
			allocationEvent, err := decodeRoomAllocationEvent(msg.Message)
			if err != nil {
				slog.Error(fmt.Sprintf("failed to decode allocation event: %v", err))
				return
			}
			select {
			case ch <- *allocationEvent:
			default:
				slog.Error(fmt.Sprintf("room allocated but channel is full: %+v", allocationEvent))
			}
		},
		OnSubscription: func(s rueidis.PubSubSubscription) {
			close(subscribed)
		},
	})
	channel := redisPubSubChannelContainer(c.keyPrefix, c.fleetName, c.containerID)
	cmd := c.client.B().Subscribe().Channel(channel).Build()
	if err := dc.Do(c.stopCtx, cmd).Error(); err != nil {
		stopDedicatedClient()
		return nil, fmt.Errorf("failed to subscribe to channel '%s': %w", channel, err)
	}
	go func() {
		defer stopDedicatedClient()
		select {
		case <-c.stopCtx.Done():
		case <-pubsubClosed:
		}
	}()

	// Wait for the subscription to be established before returning the channel.
	select {
	case <-c.stopCtx.Done():
		return nil, c.stopCtx.Err()
	case err := <-pubsubClosed:
		return nil, err
	case <-subscribed:
		return ch, nil
	}
}

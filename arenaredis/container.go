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
	client      rueidis.DedicatedClient
	keyPrefix   string
	stopCtx     context.Context
	stopFunc    context.CancelFunc
}

func newContainer(client rueidis.Client, keyPrefix string, req arena.AddContainerRequest) *container {
	stopCtx, stopFunc := context.WithCancel(context.Background())
	dc, releaseDedicatedClient := client.Dedicate()
	return &container{
		containerID: req.ContainerID,
		fleetName:   req.FleetName,
		client:      dc,
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
	received, err := subscribe(c.stopCtx, c.client, channel)
	if err != nil {
		return nil, err
	}
	go func() {
		for {
			select {
			case <-c.stopCtx.Done():
				return
			case msg := <-received:
				ev, err := decodeToContainerEvent(msg)
				if err != nil {
					slog.Error(fmt.Sprintf("failed to decode allocation event: %v", err))
					return
				}
				select {
				case ch <- ev:
				default:
					slog.Error(fmt.Sprintf("toContainer event received but channel is full: %+v", ev))
				}
			}
		}
	}()
	return ch, nil
}

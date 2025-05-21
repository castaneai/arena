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

type container struct {
	fleetName string
	address   string
	client    rueidis.Client
	keyPrefix string
	stopCtx   context.Context
	stopFunc  context.CancelFunc
}

func newContainer(client rueidis.Client, keyPrefix string, req arena.AddContainerRequest) *container {
	stopCtx, stopFunc := context.WithCancel(context.Background())
	return &container{
		fleetName: req.FleetName,
		address:   req.Address,
		client:    client,
		keyPrefix: keyPrefix,
		stopCtx:   stopCtx,
		stopFunc:  stopFunc,
	}
}

func (c *container) stop() {
	c.stopFunc()
}

// start uses Redis Streams to receive events occurring in a specific Room in realtime.
func (c *container) start(req arena.AddContainerRequest) (<-chan arena.AllocationEvent, error) {
	ch := make(chan arena.AllocationEvent, defaultAllocationChannelBufferSize)
	go func() {
		for {
			select {
			case <-c.stopCtx.Done():
				return
			default:
				channel := redisPubSubChannelContainer(c.keyPrefix, req.FleetName, req.Address)
				cmd := c.client.B().Subscribe().Channel(channel).Build()
				if err := c.client.Receive(c.stopCtx, cmd, func(msg rueidis.PubSubMessage) {
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
				}); err != nil {
					if c.stopCtx.Err() != nil {
						return
					}
					slog.Error(fmt.Sprintf("failed to subscribe to channel '%s': %v", channel, err))
					time.Sleep(time.Second)
				}
			}
		}
	}()
	return ch, nil
}

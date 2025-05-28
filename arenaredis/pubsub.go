package arenaredis

import (
	"context"
	"fmt"

	"github.com/redis/rueidis"
)

func subscribe(ctx context.Context, c rueidis.Client, channel string) (<-chan string, context.CancelFunc, error) {
	dc, closeDedicatedClient := c.Dedicate()
	subscribed := make(chan struct{})
	received := make(chan string)
	closed := dc.SetPubSubHooks(rueidis.PubSubHooks{
		OnMessage: func(msg rueidis.PubSubMessage) {
			if msg.Channel == channel {
				received <- msg.Message
			}
		},
		OnSubscription: func(_ rueidis.PubSubSubscription) {
			close(subscribed)
		},
	})
	cmd := c.B().Subscribe().Channel(channel).Build()
	if err := dc.Do(ctx, cmd).Error(); err != nil {
		closeDedicatedClient()
		return nil, nil, fmt.Errorf("failed to subscribe to channel '%s': %w", channel, err)
	}
	go func() {
		defer closeDedicatedClient()
		select {
		case <-ctx.Done():
		case <-closed:
		}
	}()

	// Wait for subscription to be confirmed.
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case err := <-closed:
		return nil, nil, fmt.Errorf("subscription has been closed '%s': %w", channel, err)
	case <-subscribed:
	}
	return received, closeDedicatedClient, nil
}

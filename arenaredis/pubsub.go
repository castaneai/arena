package arenaredis

import (
	"context"
	"fmt"

	"github.com/redis/rueidis"
)

func subscribe(ctx context.Context, c rueidis.DedicatedClient, pubsubChannelName string) (<-chan string, error) {
	subscribed := make(chan struct{})
	received := make(chan string)
	// > wait channel is guaranteed to be close when the hooks will not be called anymore,
	// > and produce at most one error describing the reason.
	// > Users can use this channel to detect disconnection.
	// https://pkg.go.dev/github.com/rueian/rueidis#readme-alternative-pubsub-hooks
	wait := c.SetPubSubHooks(rueidis.PubSubHooks{
		OnMessage: func(msg rueidis.PubSubMessage) {
			if msg.Channel == pubsubChannelName {
				received <- msg.Message
			}
		},
		OnSubscription: func(_ rueidis.PubSubSubscription) {
			close(subscribed)
		},
	})
	cmd := c.B().Subscribe().Channel(pubsubChannelName).Build()
	if err := c.Do(ctx, cmd).Error(); err != nil {
		return nil, fmt.Errorf("failed to subscribe to channel '%s': %w", pubsubChannelName, err)
	}

	// Wait for subscription to be confirmed.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-wait:
		return nil, fmt.Errorf("subscription has been closed '%s': %w", pubsubChannelName, err)
	case <-subscribed:
	}
	return received, nil
}

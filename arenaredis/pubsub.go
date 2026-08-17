package arenaredis

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/redis/rueidis"
)

// defaultReceiveBufferSize is how many messages of a container can wait for its listener before
// they start being dropped.
const defaultReceiveBufferSize = 1024

func subscribe(ctx context.Context, c rueidis.DedicatedClient, pubsubChannelName string) (<-chan string, error) {
	subscribed := make(chan struct{})
	received := make(chan string, defaultReceiveBufferSize)
	// > wait channel is guaranteed to be close when the hooks will not be called anymore,
	// > and produce at most one error describing the reason.
	// > Users can use this channel to detect disconnection.
	// https://pkg.go.dev/github.com/rueian/rueidis#readme-alternative-pubsub-hooks
	wait := c.SetPubSubHooks(rueidis.PubSubHooks{
		OnMessage: func(msg rueidis.PubSubMessage) {
			if msg.Channel != pubsubChannelName {
				return
			}
			// This hook runs on the only read loop of this connection, so blocking here stops
			// every reply on it, including the UNSUBSCRIBE that releasing the connection waits
			// for with a context that cannot be cancelled. Dropping a message costs one event;
			// blocking costs the connection, and the caller releasing it along with it.
			select {
			case received <- msg.Message:
			default:
				slog.Error(fmt.Sprintf("message received but the receive buffer of channel '%s' is full, dropping it", pubsubChannelName))
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

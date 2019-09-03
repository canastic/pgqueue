package pgqueue

import (
	"context"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/tcard/gock"
	"golang.org/x/xerrors"
)

func TestListenUnlisten(t *testing.T) {
	prevPQNewListener := pqNewListener
	defer func() { pqNewListener = prevPQNewListener }()

	prevTimeSleep := timeSleep
	defer func() { timeSleep = prevTimeSleep }()

	type channelCall struct {
		channel string
		err     chan error
	}

	listenCalls := make(chan channelCall)
	unlistenCalls := make(chan channelCall)

	pqListener := (&PQListenerMocker{
		Listen: func(channel string) error {
			c := channelCall{
				channel: channel,
				err:     make(chan error),
			}
			listenCalls <- c
			return <-c.err
		},
		Unlisten: func(channel string) error {
			c := channelCall{
				channel: channel,
				err:     make(chan error),
			}
			unlistenCalls <- c
			return <-c.err
		},
	}).Mock()

	pqNewListener = func(
		name string,
		minReconnectInterval time.Duration,
		maxReconnectInterval time.Duration,
		eventCallback pq.EventCallbackType,
	) PQListener {
		return pqListener
	}

	sleepCalls := make(chan chan struct{})

	timeSleep = func(time.Duration) {
		goOn := make(chan struct{})
		sleepCalls <- goOn
		<-goOn
	}

	t.Run("just listen", func(t *testing.T) {
		l := NewListener("", 0, 0, nil)

		err := gock.Wait(func() error {
			return l.Listen(context.Background(), "foo")
		}, func() error {
			c := <-listenCalls
			assert.Equal(t, "foo", c.channel)
			c.err <- nil
			return nil
		})
		assert.NoError(t, err)
	})

	t.Run("listen after unlisten", func(t *testing.T) {
		l := NewListener("", 0, 0, nil)

		err := gock.Wait(func() error {
			l.Unlisten("foo")
			return l.Listen(context.Background(), "foo")
		}, func() error {
			c := <-unlistenCalls
			assert.Equal(t, "foo", c.channel)
			c.err <- nil

			c = <-listenCalls
			assert.Equal(t, "foo", c.channel)
			c.err <- nil

			return nil
		})
		assert.NoError(t, err)
	})

	t.Run("listen failure on channel already opened", func(t *testing.T) {
		l := NewListener("", 0, 0, nil)

		err := gock.Wait(func() error {
			l.Unlisten("bar")
			return l.Listen(context.Background(), "foo")
		}, func() error {
			c := <-unlistenCalls
			assert.Equal(t, "bar", c.channel)
			c.err <- nil

			c = <-listenCalls
			assert.Equal(t, "foo", c.channel)
			c.err <- pq.ErrChannelAlreadyOpen

			return nil
		})
		assert.True(t, xerrors.Is(err, pq.ErrChannelAlreadyOpen))
	})

	t.Run("listen eventually ok if unlistening channel", func(t *testing.T) {
		l := NewListener("", 0, 0, nil)

		err := gock.Wait(func() error {
			l.Unlisten("foo")
			return l.Listen(context.Background(), "foo")
		}, func() error {
			c := <-unlistenCalls
			assert.Equal(t, "foo", c.channel)
			c.err <- nil

			for i := 0; i < 3; i++ {
				c = <-listenCalls
				assert.Equal(t, "foo", c.channel)
				c.err <- pq.ErrChannelAlreadyOpen

				goOn := <-sleepCalls
				close(goOn)
			}

			c = <-listenCalls
			assert.Equal(t, "foo", c.channel)
			c.err <- nil

			return nil
		})
		assert.NoError(t, err)
	})

	t.Run("listen fails again once channel is unlistened", func(t *testing.T) {
		l := NewListener("", 0, 0, nil)

		err := gock.Wait(func() error {
			l.Unlisten("foo")
			l.Listen(context.Background(), "foo")
			return l.Listen(context.Background(), "foo")
		}, func() error {
			c := <-unlistenCalls
			assert.Equal(t, "foo", c.channel)
			c.err <- nil

			c = <-listenCalls
			assert.Equal(t, "foo", c.channel)
			c.err <- pq.ErrChannelAlreadyOpen

			goOn := <-sleepCalls
			close(goOn)

			c = <-listenCalls
			assert.Equal(t, "foo", c.channel)
			c.err <- nil

			c = <-listenCalls
			assert.Equal(t, "foo", c.channel)
			c.err <- pq.ErrChannelAlreadyOpen

			return nil
		})
		assert.True(t, xerrors.Is(err, pq.ErrChannelAlreadyOpen))
	})
}

package pgqueue

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/lib/pq"
	"gitlab.com/canastic/pgqueue/stopcontext"
	"gitlab.com/canastic/sqlx"
)

type PQNotification struct {
	// Process ID (PID) of the notifying postgres backend.
	BePid int
	// Name of the channel the notification was sent on.
	Channel string
	// Payload, or the empty string if unspecified.
	Extra string
}

type PQListener interface {
	Listen(channel string) error
	Unlisten(channel string) error
	UnlistenAll() error
	NotificationChannel() <-chan *pq.Notification
	Ping() error
	Close() error
}

type Listener struct {
	PQListener
	errEvent <-chan error
}

var pqNewListener = func(
	name string,
	minReconnectInterval time.Duration,
	maxReconnectInterval time.Duration,
	eventCallback pq.EventCallbackType,
) PQListener {
	return pq.NewListener(name, minReconnectInterval, maxReconnectInterval, eventCallback)
}

func NewListener(
	name string,
	minReconnectInterval time.Duration,
	maxReconnectInterval time.Duration,
	eventCallback pq.EventCallbackType,
) *Listener {
	errEvent := make(chan error, 1)
	gotErrEvent := false
	return &Listener{
		PQListener: pqNewListener(name, minReconnectInterval, maxReconnectInterval, func(event pq.ListenerEventType, err error) {
			if !gotErrEvent {
				switch event {
				case pq.ListenerEventDisconnected, pq.ListenerEventConnectionAttemptFailed:
					errEvent <- err
				}
			}
			if eventCallback != nil {
				eventCallback(event, err)
			}
		}),
		errEvent: errEvent,
	}
}

var timeSleep = time.Sleep

func (l *Listener) Listen(ctx context.Context, channel string) error {
	listenErr := make(chan error, 1)
	go func() { listenErr <- l.PQListener.Listen(channel) }()

	var err error
	select {
	case <-stopcontext.Stopped(ctx):
		err = ctx.Err()
	case err = <-listenErr:
	}
	return err
}

func ListenForNotificationsAsDeliveries(
	ctx context.Context,
	listener *Listener,
	channel string,
	toDeliveries func(stopcontext.Context, *PQNotification, chan<- Delivery) error,
) (AcceptFunc, error) {
	err := listener.Listen(ctx, channel)
	if err != nil {
		return nil, fmt.Errorf("listening to channel %q: %w", channel, err)
	}

	notifs := listener.NotificationChannel()

	return func(ctx stopcontext.Context, deliveries chan<- Delivery) error {
		defer listener.Unlisten(channel)
		for {
			select {
			case <-ctx.Stopped():
				return ctx.Err()
			default:
			}

			select {
			case <-ctx.Stopped():
				return ctx.Err()
			case notif, ok := <-notifs:
				if !ok {
					return nil
				}

				err := toDeliveries(ctx, (*PQNotification)(notif), deliveries)
				if err != nil {
					return fmt.Errorf("mapping Postgres notification to deliveries: %w", err)
				}
			case err := <-listener.errEvent:
				return fmt.Errorf("notifications listener connection: %w", err)
			}
		}
	}, nil
}

type OrderGuarantee struct {
	ordered bool
}

var (
	Ordered   = OrderGuarantee{ordered: true}
	Unordered = OrderGuarantee{ordered: false}
)

type SubscriptionQueries struct {
	Ordered OrderGuarantee
}

func (sq SubscriptionQueries) FetchIncomingRows(
	ctx context.Context,
	tx sqlx.Tx,
	into chan<- ScanFunc,
	baseQuery string,
	args ...interface{},
) error {
	onLock := " NOWAIT"
	if !sq.Ordered.ordered {
		onLock = " SKIP LOCKED"
	}

	// We expect error 55P03, so we insert a SAVEPOINT so that we can rollback
	// to it if that happens and avoid invalidating the transaction.
	_, err := tx.Exec(ctx, "SAVEPOINT before_incoming_rows;")
	if err != nil {
		return fmt.Errorf("declaring savepoint: %w", err)
	}

	const cursorName = "incoming_rows"

	_, err = tx.Exec(ctx, "DECLARE "+cursorName+" CURSOR FOR "+baseQuery+" FOR UPDATE"+onLock, args...)
	if err != nil {
		return fmt.Errorf("declaring cursor %q: %w", cursorName, err)
	}

	err = iterCursor(ctx, into, tx, cursorName)
	if err != nil {
		var pqErr *pq.Error
		if !errors.As(err, &pqErr) || pqErr.Code != "55P03" {
			return err
		}
		// The rows were already locked, which means that someone is already
		// processing the message.

		_, err = tx.Exec(ctx, "ROLLBACK TO SAVEPOINT before_incoming_rows;")
		if err != nil {
			return fmt.Errorf("rolling back to savepoint: %w", err)
		}
	}

	return nil
}

func (sq SubscriptionQueries) FetchPendingRows(
	ctx context.Context,
	tx sqlx.Tx,
	into chan<- ScanFunc,
	baseQuery string,
	args ...interface{},
) error {
	maybeSkipLocked := ""
	if !sq.Ordered.ordered {
		maybeSkipLocked = " SKIP LOCKED"
	}

	const cursorName = "pending_rows"

	_, err := tx.Exec(ctx, "DECLARE "+cursorName+" CURSOR FOR "+baseQuery+" FOR UPDATE"+maybeSkipLocked, args...)
	if err != nil {
		return fmt.Errorf("declaring cursor %q: %w", cursorName, err)
	}

	return iterCursor(ctx, into, tx, cursorName)
}

const MarkAsDeliveredSQL string = `
	deliveries = deliveries + 1,
	last_delivered_at = NOW() AT TIME ZONE 'UTC'
`

const UpdateLastAckSQL string = `
	last_ack_at = NOW() AT TIME ZONE 'UTC'
`

type ScanFunc = func(into ...interface{}) (ok bool, err error)

func iterCursor(ctx context.Context, into chan<- ScanFunc, tx sqlx.Tx, cursor string) error {
	var done atomicBool
	iterErr := make(chan error, 1)

	for !done.load() {
		select {
		case <-stopcontext.Stopped(ctx):
			return ctx.Err()
		default:
		}

		select {
		case <-stopcontext.Stopped(ctx):
			return ctx.Err()
		case into <- func(into ...interface{}) (bool, error) {
			// We defer actually querying until the ScanFunc is called.
			// Otherwise, since the tx is shared with the receiver, there is a
			// race between this iterator's next tx.QueryRow and the receiver's
			// use of the tx.
			//
			// Unfortunately, this makes for an awkward interface because we
			// don't know in advance if there's a next row, so ScanFunc returns
			// a bool.
			rows, err := tx.Query(ctx, "FETCH NEXT FROM "+cursor)
			if err != nil {
				iterErr <- fmt.Errorf("fetching next from cursor: %w", err)
				done.store(true)
				return false, nil
			}
			if !rows.Next() {
				done.store(true)
				return false, nil
			}

			defer func() {
				rows.Close()
				err := rows.Err()
				if err != nil {
					iterErr <- fmt.Errorf("iterating rows: %w", err)
					done.store(true)
				}

				if done.load() {
					tx.Exec(ctx, "CLOSE "+cursor)
				}
			}()

			return true, rows.Scan(into...)
		}:
		}
	}

	select {
	case err := <-iterErr:
		return err
	default:
		return nil
	}
}

type atomicBool uintptr

func (b *atomicBool) store(v bool) {
	var u uintptr = 0
	if v {
		u = 1
	}
	atomic.StoreUintptr((*uintptr)(b), u)
}

func (b *atomicBool) load() bool {
	return atomic.LoadUintptr((*uintptr)(b)) > 0
}

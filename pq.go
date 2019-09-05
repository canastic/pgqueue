package pgqueue

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/lib/pq"
	"github.com/tcard/gock"
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

type AcceptPQNotificationsFunc = func(stopcontext.Context, chan<- *pq.Notification) error

func ListenForNotifications(
	ctx context.Context,
	listener *Listener,
	channel string,
) (AcceptPQNotificationsFunc, error) {
	err := listener.Listen(ctx, channel)
	if err != nil {
		return nil, fmt.Errorf("listening to channel %q: %w", channel, err)
	}

	notifs := listener.NotificationChannel()

	return func(ctx stopcontext.Context, into chan<- *pq.Notification) error {
		defer listener.Unlisten(channel)
		for {
			select {
			case <-ctx.Stopped():
				return ctx.Err()
			default:
			}

			var notif *pq.Notification

			var ok bool
			select {
			case <-ctx.Stopped():
				return ctx.Err()
			case notif, ok = <-notifs:
				if !ok {
					return nil
				}
			case err := <-listener.errEvent:
				return fmt.Errorf("notifications listener connection: %w", err)
			}

			select {
			case <-ctx.Stopped():
				return ctx.Err()
			case into <- notif:
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

func (o OrderGuarantee) FetchIncomingRows(
	ctx context.Context,
	tx sqlx.Tx,
	into chan<- ScanFunc,
	baseQuery string,
	args ...interface{},
) error {
	onLock := " NOWAIT"
	if !o.ordered {
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

func (o OrderGuarantee) FetchPendingRows(
	ctx context.Context,
	tx sqlx.Tx,
	into chan<- ScanFunc,
	baseQuery string,
	args ...interface{},
) error {
	maybeSkipLocked := ""
	if !o.ordered {
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

type AcceptQueriesFunc = func(stopcontext.Context, chan<- QueryWithArgs) error

type PQSubscriptionDriver struct {
	DB                           sqlx.DB
	ExecInsertSubscription       func(context.Context) error
	ListenForIncomingBaseQueries func(context.Context) (AcceptQueriesFunc, error)
	PendingBaseQuery             QueryWithArgs
	RowsToDeliveries             func(context.Context, sqlx.Tx, chan<- Delivery, <-chan ScanFunc) error
	Ordered                      OrderGuarantee
}

func (drv PQSubscriptionDriver) InsertSubscription(ctx context.Context) error {
	return drv.ExecInsertSubscription(ctx)
}

type QueryWithArgs struct {
	Query string
	Args  []interface{}
}

func NewQueryWithArgs(query string, args ...interface{}) QueryWithArgs {
	return QueryWithArgs{Query: query, Args: args}
}

func (drv PQSubscriptionDriver) ListenForDeliveries(ctx context.Context) (AcceptFunc, error) {
	acceptIncoming, err := drv.ListenForIncomingBaseQueries(ctx)
	if err != nil {
		return nil, err
	}

	return func(ctx stopcontext.Context, deliveries chan<- Delivery) error {
		ctx, stop := stopcontext.WithStop(ctx)
		baseQueries := make(chan QueryWithArgs)
		return gock.Wait(func() error {
			defer close(baseQueries)
			return acceptIncoming(ctx, baseQueries)
		}, func() error {
			defer stop()
			for baseQuery := range baseQueries {
				err := drv.fetchDeliveries(ctx, deliveries, drv.Ordered.FetchIncomingRows, baseQuery.Query, baseQuery.Args...)
				if err != nil {
					return err
				}
			}
			return nil
		})
	}, nil
}

func (drv PQSubscriptionDriver) FetchPendingDeliveries(ctx stopcontext.Context, deliveries chan<- Delivery) error {
	q := drv.PendingBaseQuery
	return drv.fetchDeliveries(ctx, deliveries, drv.Ordered.FetchPendingRows, q.Query, q.Args...)
}

type fetchFunc = func(
	ctx context.Context,
	tx sqlx.Tx,
	into chan<- ScanFunc,
	baseQuery string,
	args ...interface{},
) error

func (drv PQSubscriptionDriver) fetchDeliveries(
	ctx stopcontext.Context,
	deliveries chan<- Delivery,
	fetch fetchFunc,
	query string,
	args ...interface{},
) error {
	tx, err := drv.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}

	rows := make(chan ScanFunc)
	ctx, stop := stopcontext.WithStop(ctx)

	err = gock.Wait(func() error {
		defer close(rows)
		return fetch(ctx, tx, rows, query, args...)
	}, func() error {
		defer stop()
		return drv.rowsToDeliveries(ctx, tx, deliveries, rows)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	return nil
}

func (drv PQSubscriptionDriver) rowsToDeliveries(ctx context.Context, tx sqlx.Tx, deliveries chan<- Delivery, rows <-chan ScanFunc) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	deliveriesFromRows := make(chan Delivery)
	return gock.Wait(func() error {
		defer close(deliveriesFromRows)
		return drv.RowsToDeliveries(ctx, tx, deliveriesFromRows, rows)
	}, func() error {
		defer cancel()
		for d := range deliveriesFromRows {
			acked := make(chan struct{}, 1)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case deliveries <- drv.deliverySendingAckedSignal(tx, d, acked):
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-acked:
			}
		}
		return nil
	})
}

func (drv PQSubscriptionDriver) deliverySendingAckedSignal(tx sqlx.Tx, d Delivery, acked chan<- struct{}) Delivery {
	return Delivery{
		Unwrap: d.Unwrap,
		OK: func(ctx context.Context) error {
			defer close(acked)
			return d.OK(ctx)
		},
		Requeue: func(ctx context.Context) error {
			defer close(acked)
			return d.Requeue(ctx)
		},
	}
}

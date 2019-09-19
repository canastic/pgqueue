package pgqueue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
	"github.com/tcard/gock"
	"gitlab.com/canastic/pgqueue/coro"
	"gitlab.com/canastic/pgqueue/stopcontext"
	"gitlab.com/canastic/sqlx"
)

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

func NewListener(
	name string,
	minReconnectInterval time.Duration,
	maxReconnectInterval time.Duration,
	eventCallback pq.EventCallbackType,
) *Listener {
	errEvent := make(chan error, 1)
	gotErrEvent := false
	return &Listener{
		PQListener: pq.NewListener(name, minReconnectInterval, maxReconnectInterval, func(event pq.ListenerEventType, err error) {
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

func (l *Listener) Listen(ctx context.Context, channel string) error {
	listenErr := make(chan error, 1)
	go func() { listenErr <- l.PQListener.Listen(channel) }()

	var err error
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case err = <-listenErr:
	}
	return err
}

type AcceptPQNotificationsFunc = func(context.Context, func(*pq.Notification)) error

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

	return func(ctx context.Context, yield func(*pq.Notification)) error {
		defer listener.Unlisten(channel)
		for {
			select {
			case <-stopcontext.Stopped(ctx):
				return ctx.Err()
			case notif, ok := <-notifs:
				if !ok {
					return nil
				}
				yield(notif)
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

func (o OrderGuarantee) FetchIncomingRows(
	ctx context.Context,
	tx sqlx.Tx,
	yield func(sqlx.Row),
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

	err = iterCursor(ctx, yield, tx, cursorName)
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
	yield func(sqlx.Row),
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

	return iterCursor(ctx, yield, tx, cursorName)
}

const MarkAsDeliveredSQL string = `
	deliveries = deliveries + 1,
	last_delivered_at = NOW() AT TIME ZONE 'UTC'
`

const UpdateLastAckSQL string = `
	last_ack_at = NOW() AT TIME ZONE 'UTC'
`

func iterCursor(ctx context.Context, yield func(sqlx.Row), tx sqlx.Tx, cursor string) (err error) {
	defer tx.Exec(ctx, "CLOSE "+cursor)

	scanErr := make(chan error, 1)

	for {
		select {
		case <-stopcontext.Stopped(ctx):
		default:
		}

		rows, err := tx.Query(ctx, "FETCH NEXT FROM "+cursor)
		if err != nil {
			return fmt.Errorf("fetching next from cursor: %w", err)
		}
		if !rows.Next() {
			return nil
		}

		yield(rowsCloseAfterScan{rows, scanErr})
		if err := <-scanErr; err != nil {
			return fmt.Errorf("scanning: %w", err)
		}
	}
}

type rowsCloseAfterScan struct {
	rows sqlx.Rows
	err  chan<- error
}

func (r rowsCloseAfterScan) Scan(into ...interface{}) error {
	defer func() {
		r.rows.Close()
		err := r.rows.Err()
		if err != nil {
			r.err <- fmt.Errorf("iterating rows: %w", err)
		}
		r.err <- nil
	}()

	return r.rows.Scan(into...)
}

type AcceptQueriesFunc = func(context.Context, func(QueryWithArgs)) error

type PQSubscriptionDriver struct {
	DB                           sqlx.DB
	ExecInsertSubscription       func(context.Context) error
	ListenForIncomingBaseQueries func(context.Context) (AcceptQueriesFunc, error)
	PendingBaseQuery             QueryWithArgs
	RowsToDeliveries             func(context.Context, sqlx.Tx, func(Delivery), *RowIterator) error
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

	return func(ctx context.Context, yield func(Delivery)) error {
		coroCtx, cancel := context.WithCancel(ctx)
		g, wait := gock.Bundle()
		queries := NewQueryWithArgsIterator(g, func(yield func(QueryWithArgs)) error {
			return acceptIncoming(ctx, yield)
		}, coro.KillOnContextDone(coroCtx))
		g(func() error {
			defer cancel()
			for queries.Next() {
				baseQuery := queries.Yielded
				err := drv.fetchDeliveries(ctx, yield, drv.Ordered.FetchIncomingRows, baseQuery.Query, baseQuery.Args...)
				if err != nil {
					return err
				}
			}
			return nil
		})
		return wait()
	}, nil
}

func (drv PQSubscriptionDriver) FetchPendingDeliveries(ctx context.Context, yield func(Delivery)) error {
	q := drv.PendingBaseQuery
	return drv.fetchDeliveries(ctx, yield, drv.Ordered.FetchPendingRows, q.Query, q.Args...)
}

type fetchFunc = func(
	ctx context.Context,
	tx sqlx.Tx,
	yield func(sqlx.Row),
	baseQuery string,
	args ...interface{},
) error

func (drv PQSubscriptionDriver) fetchDeliveries(
	ctx context.Context,
	yield func(Delivery),
	fetch fetchFunc,
	query string,
	args ...interface{},
) (err error) {
	tx, err := drv.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}

	coroCtx, cancel := context.WithCancel(ctx)
	g, wait := gock.Bundle()
	rows := NewRowIterator(g, func(yield func(sqlx.Row)) error {
		defer func() {
			if r := recover(); r != nil {
				panic(r)
			}
		}()
		return fetch(ctx, tx, yield, query, args...)
	}, coro.KillOnContextDone(coroCtx))
	g(func() error {
		defer cancel()
		return drv.rowsToDeliveries(ctx, tx, yield, rows)
	})

	defer func() {
		r := recover()

		// If this iterator has died, finish the transaction anyway.
		var errKilled error
		if r != nil {
			if err, ok := r.(error); ok && errors.As(err, &coro.ErrKilled{}) {
				errKilled = err
			} else {
				panic(r)
			}
		}

		if ctx.Err() != nil {
			// The transaction should've killed by the context cancel already.
			return
		}

		if err != nil {
			tx.Rollback()
			return
		}

		err = tx.Commit()
		if err != nil {
			err = fmt.Errorf("committing transaction: %w", err)
			return
		}

		if errKilled != nil {
			panic(err)
		}
	}()

	return wait()
}

func (drv PQSubscriptionDriver) rowsToDeliveries(ctx context.Context, tx sqlx.Tx, yield func(Delivery), rows *RowIterator) error {
	return drv.RowsToDeliveries(ctx, tx, func(d Delivery) {
		yield(d) //DeliveryWithAckedHook(d, acked))
	}, rows)
}

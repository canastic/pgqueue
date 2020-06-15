package pgqueue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
	"github.com/tcard/coro"
	"github.com/tcard/gock"
	"github.com/tcard/sqlcoro"
	"github.com/tcard/sqler"
	"gitlab.com/canastic/pgqueue/stopcontext"
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
) (accept AcceptPQNotificationsFunc, close func() error, err error) {
	err = listener.Listen(ctx, channel)
	if err != nil {
		return nil, nil, fmt.Errorf("listening to channel %q: %w", channel, err)
	}

	notifs := listener.NotificationChannel()

	return func(ctx context.Context, yield func(*pq.Notification)) error {
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
		}, func() error {
			return listener.Close()
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
	yieldDelivery func(Delivery),
	beginRowDelivery beginRowDeliveryFunc,
	query func(context.Context, sqler.Queryer, func(baseQuery string) string) (sqler.Rows, error),
) error {
	onLock := " NOWAIT"
	if !o.ordered {
		onLock = " SKIP LOCKED"
	}
	err := queryDeliveries(ctx, yieldDelivery, beginRowDelivery, func(ctx context.Context, q sqler.Queryer) (sqler.Rows, error) {
		rows, err := query(ctx, q, func(baseQuery string) string {
			return baseQuery + " FOR UPDATE" + onLock
		})
		return rows, err
	})
	if err != nil {
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == "55P03" {
			// The rows were already locked, which means that someone is already
			// processing the message.

			err = nil
		}
	}
	return err
}

func (o OrderGuarantee) FetchPendingRows(
	ctx context.Context,
	yieldDelivery func(Delivery),
	beginRowDelivery beginRowDeliveryFunc,
	query func(context.Context, sqler.Queryer, func(baseQuery string) string) (sqler.Rows, error),
) error {
	maybeSkipLocked := ""
	if !o.ordered {
		maybeSkipLocked = " SKIP LOCKED"
	}
	return queryDeliveries(ctx, yieldDelivery, beginRowDelivery, func(ctx context.Context, q sqler.Queryer) (sqler.Rows, error) {
		return query(ctx, q, func(baseQuery string) string {
			return baseQuery + " FOR UPDATE" + maybeSkipLocked
		})
	})
}

const MarkAsDeliveredSQL string = `
	deliveries = deliveries + 1,
	last_delivered_at = NOW() AT TIME ZONE 'UTC'
`

const UpdateLastAckSQL string = `
	last_ack_at = NOW() AT TIME ZONE 'UTC'
`

func queryDeliveries(
	ctx context.Context,
	yieldDelivery func(Delivery),
	beginRowDelivery beginRowDeliveryFunc,
	query func(context.Context, sqler.Queryer) (sqler.Rows, error),
) (err error) {
	var lastTx sqler.Tx
	defer func() {
		// If the delivery we're yielding is never read because of a finished
		// consumer, yield will panic and we need to make sure we don't leak
		// the delivery's transaction.
		if lastTx != nil {
			lastTx.Rollback()
		}
	}()

	for {
		select {
		case <-stopcontext.Stopped(ctx):
		default:
		}

		tx, yield, err := beginRowDelivery(ctx)
		lastTx = tx
		if err != nil {
			return fmt.Errorf("beginning transaction to handle delivery: %w", err)
		}

		rows, err := query(ctx, tx)
		if err != nil {
			return fmt.Errorf("fetching next delivery from cursor: %w", err)
		}

		// We don't want to make a delivery if there are no more rows. So we
		// need to peek, ensuring that the next call to rows.Next doesn't
		// advance the cursor.
		hasNext, rows := peekRows(rows)
		if !hasNext {
			return nil
		}

		finishTxErr := make(chan error, 1)
		finishTx := func(do func(context.Context) error) func(context.Context) error {
			return func(ctx context.Context) (err error) {
				defer func() {
					finishTxErr <- err
				}()

				err = do(ctx)
				if err != nil {
					tx.Rollback()
					return err
				}
				err = tx.Commit()
				if err != nil {
					return fmt.Errorf("committing delivery transaction: %w", err)
				}
				return nil
			}
		}

		yield(DeliveryRows{
			Tx:   tx,
			Rows: rows,
			Deliver: func(d Delivery) {
				yieldDelivery(Delivery{
					Unwrap:  d.Unwrap,
					OK:      finishTx(d.OK),
					Requeue: finishTx(d.Requeue),
				})
			},
		})

		txFinished := false
		select {
		case err, txFinished = <-finishTxErr:
		default:
			select {
			case <-stopcontext.Stopped(ctx):
			case err, txFinished = <-finishTxErr:
			}
		}
		if txFinished {
			lastTx = nil
			if err != nil {
				return fmt.Errorf("finishing delivery transaction: %w", err)
			}
		}
	}
}

type AcceptQueriesFunc = func(context.Context, func(QueryWithArgs)) error

type PQSubscriptionDriver struct {
	DB                           sqler.DB
	ExecInsertSubscription       func(context.Context) error
	ListenForIncomingBaseQueries func(context.Context) (accept AcceptQueriesFunc, close func() error, err error)
	PendingBaseQuery             QueryWithArgs
	RowsToDeliveries             func(context.Context, *DeliveryRowsIterator) error
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

func (drv PQSubscriptionDriver) ListenForDeliveries(ctx context.Context) (accept AcceptFunc, close func() error, err error) {
	acceptIncoming, close, err := drv.ListenForIncomingBaseQueries(ctx)
	if err != nil {
		return nil, nil, err
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
	}, close, nil
}

func (drv PQSubscriptionDriver) FetchPendingDeliveries(ctx context.Context, yield func(Delivery)) error {
	q := drv.PendingBaseQuery
	return drv.fetchDeliveries(ctx, yield, drv.Ordered.FetchPendingRows, q.Query, q.Args...)
}

// DeliveryRows is a row for a delivery with a transaction to ACK it.
type DeliveryRows struct {
	Rows sqler.Rows
	sqler.Tx
	Deliver func(Delivery)
}

type NextRowFunc sqlcoro.NextFunc

type beginRowDeliveryFunc = func(context.Context) (sqler.Tx, func(DeliveryRows), error)

type fetchFunc = func(
	ctx context.Context,
	yieldDelivery func(Delivery),
	beginRowDelivery beginRowDeliveryFunc,
	query func(context.Context, sqler.Queryer, func(baseQuery string) string) (sqler.Rows, error),
) error

func (drv PQSubscriptionDriver) beginRowDelivery(yield func(DeliveryRows)) beginRowDeliveryFunc {
	return func(ctx context.Context) (sqler.Tx, func(DeliveryRows), error) {
		tx, err := drv.DB.BeginTx(ctx, nil)
		return tx, yield, err
	}
}

func (drv PQSubscriptionDriver) fetchDeliveries(
	ctx context.Context,
	yieldDelivery func(Delivery),
	fetch fetchFunc,
	baseQuery string,
	args ...interface{},
) (err error) {
	coroCtx, cancel := context.WithCancel(ctx)
	g, wait := gock.Bundle()
	rows := NewDeliveryRowsIterator(g, func(yield func(DeliveryRows)) error {
		return fetch(ctx, yieldDelivery, drv.beginRowDelivery(yield), func(ctx context.Context, q sqler.Queryer, query func(baseQuery string) string) (sqler.Rows, error) {
			return q.Query(ctx, query(baseQuery), args...)
		})
	}, coro.KillOnContextDone(coroCtx))
	g(func() error {
		defer cancel()
		return drv.RowsToDeliveries(ctx, rows)
	})

	return wait()
}

func peekRows(rows sqler.Rows) (hasNext bool, _ sqler.Rows) {
	hasNext = rows.Next()
	return hasNext, &rowsSkipFirstNext{Rows: rows}
}

type rowsSkipFirstNext struct {
	sqler.Rows
	skipped bool
}

func (r *rowsSkipFirstNext) Next() bool {
	if !r.skipped {
		r.skipped = true
		return true
	}
	return r.Rows.Next()
}

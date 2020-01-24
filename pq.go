package pgqueue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
	"github.com/tcard/coro"
	"github.com/tcard/gock"
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
	yieldDelivery func(Delivery),
	beginRowDelivery beginRowDeliveryFunc,
	query func(context.Context, sqlx.Queryer, func(baseQuery string) string) (sqlx.Rows, error),
) error {
	onLock := " NOWAIT"
	if !o.ordered {
		onLock = " SKIP LOCKED"
	}
	err := queryDeliveries(ctx, yieldDelivery, beginRowDelivery, func(ctx context.Context, q sqlx.Queryer) (sqlx.Rows, error) {
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
	query func(context.Context, sqlx.Queryer, func(baseQuery string) string) (sqlx.Rows, error),
) error {
	maybeSkipLocked := ""
	if !o.ordered {
		maybeSkipLocked = " SKIP LOCKED"
	}
	return queryDeliveries(ctx, yieldDelivery, beginRowDelivery, func(ctx context.Context, q sqlx.Queryer) (sqlx.Rows, error) {
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
	query func(context.Context, sqlx.Queryer) (sqlx.Rows, error),
) (err error) {
	scanErr := make(chan error, 1)

	for {
		select {
		case <-stopcontext.Stopped(ctx):
		default:
		}

		tx, yield, err := beginRowDelivery(ctx)
		if err != nil {
			return fmt.Errorf("beginning transaction to handle delivery: %w", err)
		}

		rows, err := query(ctx, tx)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("fetching next delivery from cursor: %w", err)
		}
		if !rows.Next() {
			tx.Rollback()
			return nil
		}

		finishTx := func(do func(context.Context) error) func(context.Context) error {
			return func(ctx context.Context) error {
				err := do(ctx)
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

		yield(DeliveryRow{
			Tx:  tx,
			Row: rowsCloseAfterScan{rows, scanErr},
			Deliver: func(d Delivery) {
				yieldDelivery(Delivery{
					Unwrap:  d.Unwrap,
					OK:      finishTx(d.OK),
					Requeue: finishTx(d.Requeue),
				})
			},
		})
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
	RowsToDeliveries             func(context.Context, *DeliveryRowIterator) error
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

// DeliveryRow is a row for a delivery with a transaction to ACK it.
type DeliveryRow struct {
	sqlx.Row
	sqlx.Tx
	Deliver func(Delivery)
}

type beginRowDeliveryFunc = func(context.Context) (sqlx.Tx, func(DeliveryRow), error)

type fetchFunc = func(
	ctx context.Context,
	yieldDelivery func(Delivery),
	beginRowDelivery beginRowDeliveryFunc,
	query func(context.Context, sqlx.Queryer, func(baseQuery string) string) (sqlx.Rows, error),
) error

func (drv PQSubscriptionDriver) beginRowDelivery(yield func(DeliveryRow)) beginRowDeliveryFunc {
	return func(ctx context.Context) (sqlx.Tx, func(DeliveryRow), error) {
		tx, err := drv.DB.BeginTx(ctx, nil)
		if err != nil {
			return nil, nil, fmt.Errorf("beginning transaction to handle delivery: %w", err)
		}
		return tx, yield, nil
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
	rows := NewDeliveryRowIterator(g, func(yield func(DeliveryRow)) error {
		defer func() {
			if r := recover(); r != nil {
				panic(r)
			}
		}()
		return fetch(ctx, yieldDelivery, drv.beginRowDelivery(func(r DeliveryRow) {
			yield(r)
		}), func(ctx context.Context, q sqlx.Queryer, query func(baseQuery string) string) (sqlx.Rows, error) {
			return q.Query(ctx, query(baseQuery)+" LIMIT 1", args...)
		})
	}, coro.KillOnContextDone(coroCtx))
	g(func() error {
		defer cancel()
		return drv.RowsToDeliveries(ctx, rows)
	})

	return wait()
}

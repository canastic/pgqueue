package pgqueue

import (
	"context"

	"github.com/lib/pq"
	"gitlab.com/canastic/pgqueue/stopcontext"
	"gitlab.com/canastic/sqlx"
	"golang.org/x/xerrors"
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
	NotificationChannel() <-chan *PQNotification
	Close() error
}

func ListenForNotificationsAsDeliveries(
	ctx context.Context,
	listener PQListener,
	channel string,
	toDeliveries func(stopcontext.Context, *PQNotification, chan<- Delivery) error,
) (AcceptFunc, error) {
	listenErr := make(chan error, 1)
	go func() { listenErr <- listener.Listen(channel) }()
	select {
	case <-stopcontext.Stopped(ctx):
		return nil, ctx.Err()
	case err := <-listenErr:
		if err != nil {
			return nil, xerrors.Errorf("listening to channel %q: %w", channel, err)
		}
	}

	notifs := listener.NotificationChannel()

	return func(ctx stopcontext.Context, deliveries chan<- Delivery) error {
		defer listener.Unlisten(channel)
		for {
			select {
			case <-ctx.Stopped():
				return ctx.Err()
			case notif, ok := <-notifs:
				if !ok {
					return nil
				}

				err := toDeliveries(ctx, notif, deliveries)
				if err != nil {
					return xerrors.Errorf("mapping Postgres notification to deliveries: %w", err)
				}
			}
		}
	}, nil
}

func BridgePQListener(l *pq.Listener) PQListener {
	return pqListenerBridge{l}
}

type pqListenerBridge struct {
	l *pq.Listener
}

func (l pqListenerBridge) Listen(channel string) error {
	return l.l.Listen(channel)
}

func (l pqListenerBridge) Unlisten(channel string) error {
	return l.l.Unlisten(channel)
}

func (l pqListenerBridge) NotificationChannel() <-chan *PQNotification {
	ch := make(chan *PQNotification)
	go func() {
		defer close(ch)
		for notif := range l.l.NotificationChannel() {
			ch <- (*PQNotification)(notif)
		}
	}()
	return ch
}

func (l pqListenerBridge) Close() error {
	return l.l.Close()
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
		return xerrors.Errorf("declaring savepoint: %w", err)
	}

	const cursorName = "incoming_rows"

	_, err = tx.Exec(ctx, "DECLARE "+cursorName+" CURSOR FOR "+baseQuery+" FOR UPDATE"+onLock, args...)
	if err != nil {
		return xerrors.Errorf("declaring cursor %q: %w", cursorName, err)
	}

	err = iterCursor(ctx, into, tx, cursorName)
	if err != nil {
		var pqErr *pq.Error
		if !xerrors.As(err, &pqErr) || pqErr.Code != "55P03" {
			return err
		}
		// The rows were already locked, which means that someone is already
		// processing the message.

		_, err = tx.Exec(ctx, "ROLLBACK TO SAVEPOINT before_incoming_rows;")
		if err != nil {
			return xerrors.Errorf("rolling back to savepoint: %w", err)
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
		return xerrors.Errorf("declaring cursor %q: %w", cursorName, err)
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
	done := false
	var iterErr error

	for !done {
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
				iterErr = xerrors.Errorf("fetching next from cursor: %w", err)
				done = true
				return false, nil
			}
			if !rows.Next() {
				done = true
				return false, nil
			}

			defer func() {
				rows.Close()
				err := rows.Err()
				if err != nil {
					iterErr = xerrors.Errorf("iterating rows: %w", err)
				}

				if done {
					tx.Exec(ctx, "CLOSE "+cursor)
				}
			}()

			return true, rows.Scan(into...)
		}:
		}
	}

	return iterErr
}

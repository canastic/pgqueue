package pgqueue

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/tcard/gock"
	"gitlab.com/canastic/chantest"
	"gitlab.com/canastic/pgqueue/stopcontext"
	"gitlab.com/canastic/sqlx"
	"gitlab.com/canastic/ulidx"
	"golang.org/x/xerrors"
)

var (
	postgresConnString = os.Getenv("PGQUEUE_TEST_POSTGRES_CONNSTRING_PATTERN")
	baseDB             = os.Getenv("PGQUEUE_TEST_BASE_DB")
	enabled            = os.Getenv("PGQUEUE_TEST_ENABLED") == "true"
)

func TestPQBasicOrdered(t *testing.T) {
	if !enabled {
		t.Skip("Set PGQUEUE_TEST_ENABLED=true to run")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, cleanup := createTestDB(ctx, t)
	defer cleanup()

	type delivery struct {
		payload string
		ack     chan<- Ack
	}

	type consumer struct {
		name       string
		consume    ConsumeFunc
		deliveries chan delivery
		stop       func()
	}

	// Concurrent consumers to test mutual exclusion.
	// TODO: Bring back concurrency.
	const concurrentConsumersPerSubscription = 1

	consumers := map[string]*consumer{}

	for _, name := range []string{"foo", "bar"} {
		c := consumer{
			name: name,
			consume: func(context.Context, GetHandler) error {
				return nil
			},
			deliveries: make(chan delivery),
		}

		for i := 0; i < concurrentConsumersPerSubscription; i++ {
			l := BridgePQListener(pq.NewListener(db.connStr, time.Millisecond, time.Millisecond, nil))
			defer l.Close()

			consume, err := Subscribe(ctx, newTestPQSubscriptionDriver(db, l, name, Ordered))
			assert.NoError(t, err)

			prevConsume := c.consume
			c.consume = func(ctx context.Context, gh GetHandler) error {
				return gock.Wait(func() error {
					return prevConsume(ctx, gh)
				}, func() error {
					return consume(ctx, gh)
				})
			}
		}

		consumers[name] = &c
	}

	for i := 0; i < 3; i++ {
		err := publishMessage(ctx, db, fmt.Sprintf("pending message %d", i))
		assert.NoError(t, err)
	}

	startConsumer := func(c *consumer) {
		ctx, stop := stopcontext.WithStop(ctx)
		done := make(chan struct{}, 1)
		c.stop = func() {
			stop()
			<-done
		}

		go func() {
			defer close(done)
			err := c.consume(ctx, func() (unwrapInto interface{}, handle HandleFunc) {
				var payload string
				return &payload, func(ctx context.Context) (context.Context, Ack) {
					ack := make(chan Ack)
					c.deliveries <- delivery{payload, ack}
					return ctx, <-ack
				}
			})
			assert.NoError(t, err)
		}()
	}
	for _, c := range consumers {
		startConsumer(c)
	}

	publishedNewMessages := false
	for consumerName, c := range consumers {
		d := chantest.AssertRecv(t, c.deliveries).(delivery)
		assert.Equal(t, "pending message 0", d.payload, "consumer %q", consumerName)

		if !publishedNewMessages {
			for i := 0; i < 3; i++ {
				err := publishMessage(ctx, db, fmt.Sprintf("incoming message %d", i))
				assert.NoError(t, err, "consumer %q", consumerName)
			}
		}

		chantest.AssertSend(t, d.ack, OK, "consumer %q", consumerName)

		for _, expected := range []string{
			"pending message 1",
			"pending message 2",
			"incoming message 0",
			"incoming message 1",
			"incoming message 2",
		} {
			d := chantest.AssertRecv(t, c.deliveries, "consumer %q msg %q", consumerName, expected).(delivery)
			assert.Equal(t, expected, d.payload, "consumer %q msg %q", consumerName, expected)
			chantest.AssertSend(t, d.ack, OK, "consumer %q msg %q", consumerName, expected)
		}

		if !publishedNewMessages {
			chantest.AssertNoRecv(t, c.deliveries, "consumer %q", consumerName)
			err := publishMessage(ctx, db, "new incoming message")
			assert.NoError(t, err, "consumer %q", consumerName)
		}

		d = chantest.AssertRecv(t, c.deliveries, "consumer %q", consumerName).(delivery)
		assert.Equal(t, "new incoming message", d.payload, "consumer %q", consumerName)
		chantest.AssertSend(t, d.ack, OK, "consumer %q", consumerName)

		c.stop()
		startConsumer(c)

		if !publishedNewMessages {
			chantest.AssertNoRecv(t, c.deliveries)
			err := publishMessage(ctx, db, "incoming message after restart")
			assert.NoError(t, err, "consumer %q", consumerName)
		}

		d = chantest.AssertRecv(t, c.deliveries, "consumer %q", consumerName).(delivery)
		assert.Equal(t, "incoming message after restart", d.payload, "consumer %q", consumerName)
		chantest.AssertSend(t, d.ack, OK, "consumer %q", consumerName)

		c.stop()

		publishedNewMessages = true
	}
}

// TODO: Exhaustive failure and concurrency tests.

type testPQSubscriptionDriver struct {
	db      sqlx.DB
	l       PQListener
	name    string
	queries SubscriptionQueries
}

func newTestPQSubscriptionDriver(db sqlx.DB, l PQListener, name string, ordered OrderGuarantee) testPQSubscriptionDriver {
	return testPQSubscriptionDriver{
		db:      db,
		l:       l,
		name:    name,
		queries: SubscriptionQueries{Ordered: ordered},
	}
}

func (drv testPQSubscriptionDriver) InsertSubscription(ctx context.Context) error {
	_, err := drv.db.Exec(ctx, "INSERT INTO subscriptions (name) VALUES ($1);", drv.name)
	return ignoreUniqueViolation(err)
}

func (drv testPQSubscriptionDriver) ListenForDeliveries(ctx context.Context) (AcceptFunc, error) {
	return ListenForNotificationsAsDeliveries(ctx, drv.l, fmt.Sprintf("message:%s", drv.name), drv.notifToDeliveries)
}

func (drv testPQSubscriptionDriver) notifToDeliveries(ctx stopcontext.Context, notif *PQNotification, deliveries chan<- Delivery) error {
	return drv.fetchDeliveries(ctx, deliveries, drv.queries.FetchIncomingRows, `
		SELECT serial, payload
		FROM message_deliveries m
		WHERE
			subscription = $1
		ORDER BY serial
	`, drv.name)
}

func (drv testPQSubscriptionDriver) FetchPendingDeliveries(ctx stopcontext.Context, deliveries chan<- Delivery) error {
	return drv.fetchDeliveries(ctx, deliveries, drv.queries.FetchPendingRows, `
		SELECT serial, payload
		FROM message_deliveries m
		WHERE
			subscription = $1
		ORDER BY serial
	`, drv.name)
}

type fetchFunc = func(
	ctx context.Context,
	tx sqlx.Tx,
	into chan<- ScanFunc,
	baseQuery string,
	args ...interface{},
) error

func (drv testPQSubscriptionDriver) fetchDeliveries(
	ctx stopcontext.Context,
	deliveries chan<- Delivery,
	fetch fetchFunc,
	query string,
	args ...interface{},
) error {
	tx, err := drv.db.BeginTx(ctx, nil)
	if err != nil {
		return xerrors.Errorf("beginning transaction: %w", err)
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
	if err != nil && !xerrors.Is(err, context.Canceled) {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (drv testPQSubscriptionDriver) rowsToDeliveries(ctx context.Context, tx sqlx.Tx, deliveries chan<- Delivery, rows <-chan ScanFunc) error {
	for scanRow := range rows {
		var serial int64
		var payload string

		ok, err := scanRow(&serial, &payload)
		if err != nil {
			return xerrors.Errorf("scanning delivery: %w", err)
		}
		if !ok {
			continue
		}

		_, err = tx.Exec(ctx, `
			UPDATE message_deliveries SET `+MarkAsDeliveredSQL+`
			WHERE
				serial = $1
				AND subscription = $2
		`, serial, drv.name)
		if err != nil {
			return xerrors.Errorf("marking as delivered: %w", err)
		}

		acked := make(chan struct{}, 1)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case deliveries <- drv.toDelivery(tx, serial, payload, acked):
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-acked:
		}
	}
	return nil
}

func (drv testPQSubscriptionDriver) toDelivery(tx sqlx.Tx, serial int64, payload string, acked chan<- struct{}) Delivery {
	return Delivery{
		Unwrap: func(into interface{}) error {
			*(into.(*string)) = payload
			return nil
		},
		OK: func(ctx context.Context) error {
			defer close(acked)

			_, err := tx.Exec(ctx, `
				DELETE FROM message_deliveries
				WHERE
					serial = $1
					AND subscription = $2
			`, serial, drv.name)
			return err
		},
		Requeue: func(ctx context.Context) error {
			defer close(acked)

			_, err := tx.Exec(ctx, `
				UPDATE message_deliveries SET `+UpdateLastAckSQL+`
				WHERE
					serial = $1
					AND subscription = $2
			`, serial, drv.name)
			return err
		},
	}
}

var nextSerial int64

func publishMessage(ctx context.Context, db sqlx.DB, payload string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return xerrors.Errorf("beginning transaction: %w", err)
	}

	msgSerial := nextSerial
	nextSerial++

	_, err = tx.Exec(ctx, `
		DECLARE subscriptions_cursor CURSOR FOR
		SELECT name FROM subscriptions;
	`)
	if err != nil {
		return xerrors.Errorf("selecting subscriptions: %w", err)
	}
	defer tx.Exec(ctx, `CLOSE subscriptions_cursor;`)

	for {
		var sub string
		err := tx.QueryRow(ctx, `FETCH NEXT FROM subscriptions_cursor;`).Scan(&sub)
		if err != nil {
			if xerrors.Is(err, sql.ErrNoRows) {
				break
			}
			return xerrors.Errorf("scanning subscription name: %w", err)
		}

		_, err = tx.Exec(ctx,
			"INSERT INTO message_deliveries (serial, subscription, payload) VALUES ($1, $2, $3);",
			msgSerial, sub, payload,
		)
		if ignoreUniqueViolation(err) != nil {
			return xerrors.Errorf("inserting pending delivery for subscription %q and message %d: %w", sub, msgSerial, err)
		}
	}

	return tx.Commit()
}

func ignoreUniqueViolation(err error) error {
	var pqErr *pq.Error
	const uniqueViolation = "23505"
	if xerrors.As(err, &pqErr) && pqErr.Code == uniqueViolation {
		err = nil
	}
	return err
}

type sqlxTestDB struct {
	sqlx.DB
	connStr string
}

func createTestDB(ctx context.Context, t *testing.T) (sqlxTestDB, func()) {
	name := fmt.Sprintf("pgqueue_test_%s", strings.ToLower(ulidx.New()))

	db, err := sqlxOpen("postgres", fmt.Sprintf(postgresConnString, baseDB))
	if err != nil {
		panic(err)
	}

	t.Log("Creating database:", name)
	_, err = db.Exec(ctx, "CREATE DATABASE "+name)
	if err != nil {
		panic(err)
	}

	cleanup := func() {
		_, err := db.Exec(ctx, "DROP DATABASE "+name)
		if err != nil {
			panic(err)
		}
		t.Log("Dropped database:", name)
	}

	connStr := fmt.Sprintf(postgresConnString, name)
	testDB, err := sqlxOpen("postgres", connStr)
	if err != nil {
		cleanup()
		panic(err)
	}

	var oldCleanup = cleanup
	cleanup = func() {
		testDB.Close()
		oldCleanup()
	}

	_, err = testDB.Exec(ctx, createScript)
	if err != nil {
		cleanup()
		panic(err)
	}

	return sqlxTestDB{
		DB:      testDB,
		connStr: connStr,
	}, cleanup
}

func sqlxOpen(driverName, dataSourceName string) (sqlx.DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	return sqlx.WrapDB(db), nil
}

const createScript = `

CREATE TABLE subscriptions
(
	name text,
	CONSTRAINT subscriptions_pkey PRIMARY KEY (name)
);

CREATE TABLE message_deliveries
(
	subscription text NOT NULL,
	serial bigint NOT NULL,
	payload text NOT NULL,
	created_at timestamptz DEFAULT timezone('utc', now()),
	deliveries integer NOT NULL DEFAULT 0,
	last_delivered_at timestamptz,
	last_ack_at timestamptz,
	CONSTRAINT message_deliveries_pkey PRIMARY KEY (subscription, serial)
);

ALTER TABLE "message_deliveries"
ADD FOREIGN KEY ("subscription") REFERENCES "subscriptions" ("name") ON DELETE RESTRICT ON UPDATE RESTRICT;

CREATE FUNCTION notify_message_delivery()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    NOT LEAKPROOF
AS $BODY$
DECLARE
BEGIN
    PERFORM pg_notify('message:' || new.subscription, new.serial :: text);
    RETURN new;
END
$BODY$;

CREATE TRIGGER notify_message_delivery
    AFTER INSERT OR UPDATE
    ON message_deliveries
    FOR EACH ROW
    EXECUTE PROCEDURE notify_message_delivery();

`

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

	db, cleanupDB := createTestDB(ctx, t)
	defer cleanupDB()

	consumers, cleanupConsumers := setupConsumersTest(t, ctx, db, Ordered)
	defer cleanupConsumers()

	publishedNewMessages := false
	for consumerName, c := range consumers {
		d := chantest.AssertRecv(t, c.deliveries).(testStringDelivered)
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
			d := chantest.AssertRecv(t, c.deliveries, "consumer %q msg %q", consumerName, expected).(testStringDelivered)
			assert.Equal(t, expected, d.payload, "consumer %q msg %q", consumerName, expected)
			chantest.AssertSend(t, d.ack, OK, "consumer %q msg %q", consumerName, expected)
		}

		if !publishedNewMessages {
			chantest.AssertNoRecv(t, c.deliveries, "consumer %q", consumerName)
			err := publishMessage(ctx, db, "new incoming message")
			assert.NoError(t, err, "consumer %q", consumerName)
		}

		d = chantest.AssertRecv(t, c.deliveries, "consumer %q", consumerName).(testStringDelivered)
		assert.Equal(t, "new incoming message", d.payload, "consumer %q", consumerName)
		chantest.AssertSend(t, d.ack, OK, "consumer %q", consumerName)

		if publishedNewMessages {
			// Let's consume next expected message but not ACK it, to test it
			// that we get it delivered again when we restart the consumers.
			d := chantest.AssertRecv(t, c.deliveries, "consumer %q", consumerName).(testStringDelivered)
			assert.Equal(t, "incoming message after restart", d.payload, "consumer %q", consumerName)
		}
		c.stop()
		startTestConsumer(t, ctx, c)

		if !publishedNewMessages {
			chantest.AssertNoRecv(t, c.deliveries)
			err := publishMessage(ctx, db, "incoming message after restart")
			assert.NoError(t, err, "consumer %q", consumerName)
		}

		d = chantest.AssertRecv(t, c.deliveries, "consumer %q", consumerName).(testStringDelivered)
		assert.Equal(t, "incoming message after restart", d.payload, "consumer %q", consumerName)
		chantest.AssertSend(t, d.ack, OK, "consumer %q", consumerName)

		c.stop()

		publishedNewMessages = true
	}
}

func setupConsumersTest(t *testing.T, ctx context.Context, db sqlxTestDB, order OrderGuarantee) (consumers map[string]*testConsumer, cleanup func()) {
	// Concurrent consumers to test mutual exclusion.
	const concurrentConsumersPerSubscription = 2

	consumers = map[string]*testConsumer{}
	cleanup = func() {}

	for _, name := range []string{"foo", "bar"} {
		var subConsume []ConsumeFunc

		c := &testConsumer{
			name: name,
			consume: func(ctx context.Context, gh GetHandler) error {
				g, wait := gock.Bundle()
				for _, consume := range subConsume {
					consume := consume
					g(func() error {
						return consume(ctx, gh)
					})
				}
				return wait()
			},
			deliveries: make(chan testStringDelivered, concurrentConsumersPerSubscription),
		}
		consumers[name] = c

		for i := 0; i < concurrentConsumersPerSubscription; i++ {
			l := BridgePQListener(pq.NewListener(db.connStr, time.Millisecond, time.Millisecond, nil))
			prevCleanup := cleanup
			cleanup = func() { l.Close(); prevCleanup() }

			consume, err := Subscribe(ctx, newTestPQSubscriptionDriver(db, l, name, order))
			assert.NoError(t, err)

			subConsume = append(subConsume, consume)
		}
	}

	for i := 0; i < 3; i++ {
		err := publishMessage(ctx, db, fmt.Sprintf("pending message %d", i))
		assert.NoError(t, err)
	}

	for _, c := range consumers {
		startTestConsumer(t, ctx, c)
	}

	return consumers, cleanup
}

func startTestConsumer(t *testing.T, ctx context.Context, c *testConsumer) {
	t.Helper()

	ctx, cancel := context.WithCancel(ctx)
	ctx, stop := stopcontext.WithStop(ctx)
	done := make(chan struct{}, 1)
	c.stop = func() {
		t.Helper()

		stop()
		select {
		case <-done:
		case <-time.After(1 * time.Second):
			t.Errorf("had to cancel consumer %q", c.name)
			cancel()
		}
	}

	go func() {
		defer close(done)
		err := c.consume(ctx, func() (unwrapInto interface{}, handle HandleFunc) {
			var payload string
			return &payload, func(ctx context.Context) (context.Context, Ack) {
				ack := make(chan Ack)

				select {
				case <-stopcontext.Stopped(ctx):
					return ctx, Requeue
				case c.deliveries <- testStringDelivered{payload, ack}:
				}

				select {
				case <-stopcontext.Stopped(ctx):
					return ctx, Requeue
				case ack := <-ack:
					return ctx, ack
				}
			}
		})
		if gock.AnyIs(err, ErrRequeued) {
			// Message can be requeued if consumer was stopped while a
			// delivery was being handled.
			err = nil
		}
		assert.NoError(t, err)
	}()
}

type testStringDelivered struct {
	payload string
	ack     chan<- Ack
}

type testConsumer struct {
	name       string
	consume    ConsumeFunc
	deliveries chan testStringDelivered
	stop       func()
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
		ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()
		_, err := db.Exec(ctx, "DROP DATABASE "+name)
		assert.NoError(t, err)
		if err == nil {
			t.Log("Dropped database:", name)
		}
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

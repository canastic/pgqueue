package pgqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/tcard/coro"
	"github.com/tcard/gock"
	"github.com/tcard/sqler"
	"gitlab.com/canastic/chantest"
	"gitlab.com/canastic/pgqueue/stopcontext"
	"gitlab.com/canastic/ulidx"
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

	g, wait := gock.Bundle()
	defer wait()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, cleanupDB := createTestDB(ctx, t)
	defer cleanupDB()

	// Set up a bunch of consumers. Each consumer function is actually a bunch
	// of concurrent consumers for the same subscription.
	//
	// There will be 3 queued messages: "pending message n"

	consumers, cleanupConsumers := setupConsumersTest(t, ctx, g, db, Ordered)
	defer cleanupConsumers()

	// First test: deliver the first message, then publish some incoming
	// messages, then ack the delivery. Pending messages should arrive before
	// incoming messages do.

	pendingAcks := map[*testConsumer]chan<- Ack{}
	for _, c := range consumers {
		d := chantest.Before(1*time.Second).AssertRecv(t, c.deliveries).(testStringDelivered)
		assert.Equal(t, "pending message 0", d.payload, "subscription %q", c.subscription)
		pendingAcks[c] = d.ack
	}

	for i := 0; i < 3; i++ {
		err := publishMessage(ctx, db, fmt.Sprintf("incoming message %d", i))
		assert.NoError(t, err)
	}

	for c, ack := range pendingAcks {
		chantest.AssertSend(t, ack, OK, "subscription %q", c.subscription)
	}

	// Now let's receive and ack all queued messages, in order.

	for _, c := range consumers {
		for _, expected := range []string{
			"pending message 1",
			"pending message 2",
			"incoming message 0",
			"incoming message 1",
			"incoming message 2",
		} {
			d := chantest.AssertRecv(t, c.deliveries, "subscription %q msg %q", c.subscription, expected).(testStringDelivered)
			assert.Equal(t, expected, d.payload, "subscription %q msg %q", c.subscription, expected)
			chantest.AssertSend(t, d.ack, OK, "subscription %q msg %q", c.subscription, expected)
		}

		// Now the queue should be empty.
		chantest.AssertNoRecv(t, c.deliveries, "subscription %q", c.subscription)
	}

	// Let's publish a new message once the queue is empty, and process it.

	err := publishMessage(ctx, db, "new incoming message")
	assert.NoError(t, err)

	for _, c := range consumers {
		d := chantest.AssertRecv(t, c.deliveries, "subscription %q", c.subscription).(testStringDelivered)
		assert.Equal(t, "new incoming message", d.payload, "subscription %q", c.subscription)
		chantest.AssertSend(t, d.ack, OK, "subscription %q", c.subscription)
	}

	// Let's publish and deliver a message but not ACK it, to test that we get it
	// delivered again when we restart the consumers.

	err = publishMessage(ctx, db, "incoming message after restart")
	assert.NoError(t, err)

	for _, c := range consumers {
		d := chantest.AssertRecv(t, c.deliveries, "subscription %q", c.subscription).(testStringDelivered)
		assert.Equal(t, "incoming message after restart", d.payload, "subscription %q", c.subscription)

		c.kill()
		startTestConsumer(t, ctx, g, c)

		d = chantest.AssertRecv(t, c.deliveries, "subscription %q", c.subscription).(testStringDelivered)
		assert.Equal(t, "incoming message after restart", d.payload, "subscription %q", c.subscription)
		chantest.AssertSend(t, d.ack, OK, "subscription %q", c.subscription)

		c.stop()
	}
}

func TestPQBasicUnordered(t *testing.T) {
	if !enabled {
		t.Skip("Set PGQUEUE_TEST_ENABLED=true to run")
		return
	}

	g, wait := gock.Bundle()
	defer wait()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, cleanupDB := createTestDB(ctx, t)
	defer cleanupDB()

	// Set up a bunch of consumers. Each consumer function is actually a bunch
	// of concurrent consumers for the same subscription.
	//
	// There will be 3 queued messages: "pending message n"

	consumers, cleanupConsumers := setupConsumersTest(t, ctx, g, db, Unordered)
	defer cleanupConsumers()

	// First test: ack the first message, then publish some incoming messages.

	assertsByConsumer := map[*testConsumer]receiveAssertions{}
	for _, c := range consumers {
		expected := []string{
			"pending message 0",
			"pending message 1",
			"pending message 2",
		}
		asserts := unorderedExpecter(c, expected)
		asserts.receive(t)
		assertsByConsumer[c] = asserts
	}

	for i := 0; i < 3; i++ {
		err := publishMessage(ctx, db, fmt.Sprintf("incoming message %d", i))
		assert.NoError(t, err)
	}

	// Now let's receive and ack all queued messages, in random order.

	for _, c := range consumers {
		asserts := assertsByConsumer[c]

		for range []string{
			"pending message 1",
			"pending message 2",
			"incoming message 0",
			"incoming message 1",
			"incoming message 2",
		} {
			asserts.receive(t)
		}

		// Now the queue should be empty.
		chantest.AssertNoRecv(t, c.deliveries, "subscription %q", c.subscription)
	}

	// Let's publish a new message once the queue is empty, and process it.

	err := publishMessage(ctx, db, "new incoming message")
	assert.NoError(t, err)

	for _, c := range consumers {
		asserts := assertsByConsumer[c]
		asserts.receive(t)
	}

	// Let's publish and deliver a message but not consume it, to test that we get
	// it delivered when we restart the consumers.

	err = publishMessage(ctx, db, "incoming message after restart")
	assert.NoError(t, err)

	for _, c := range consumers {
		asserts := assertsByConsumer[c]

		c.kill()
		startTestConsumer(t, ctx, g, c)

		asserts.receive(t)

		c.stop()

		asserts.allReceived(t)
	}
}

func TestPQMultipleRowsPerDelivery(t *testing.T) {
	if !enabled {
		t.Skip("Set PGQUEUE_TEST_ENABLED=true to run")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, cleanupDB := createTestDB(ctx, t)
	defer cleanupDB()

	l := NewListener(db.connStr, time.Millisecond, time.Millisecond, nil)
	defer l.Close()

	drv := newTestPQSubscriptionDriver(db, l, "test", Ordered)

	consume, err := Subscribe(ctx, drv)
	assert.NoError(t, err)

	for i := 0; i < 5; i++ {
		err := publishMessage(ctx, db, fmt.Sprintf("msg%d", i))
		assert.NoError(t, err)
	}

	got := make(chan []string, 10)
	ack := make(chan Ack, 1)

	{
		ctx, cancel := context.WithCancel(ctx)

		assert.Error(t, context.Canceled, gock.Wait(func() error {
			return consume(ctx, func() (unwrapInto interface{}, handle HandleFunc) {
				messages := make([]string, 0, 3)
				return &messages, func(ctx context.Context) (context.Context, Ack) {
					got <- messages
					return ctx, <-ack
				}
			})
		}, func() error {
			defer cancel()

			msgs := chantest.Before(1*time.Second).AssertRecv(t, got).([]string)
			assert.Equal(t, []string{"msg0", "msg1", "msg2"}, msgs)

			chantest.AssertSend(t, ack, OK)

			msgs = chantest.Before(1*time.Second).AssertRecv(t, got).([]string)
			assert.Equal(t, []string{"msg3", "msg4"}, msgs)

			chantest.AssertSend(t, ack, OK)

			err := publishMessage(ctx, db, "msg5")
			assert.NoError(t, err)

			msgs = chantest.Before(1*time.Second).AssertRecv(t, got).([]string)
			assert.Equal(t, []string{"msg5"}, msgs)

			chantest.AssertSend(t, ack, OK)

			chantest.AssertNoRecv(t, got)

			return nil
		}))
		chantest.AssertNoRecv(t, got)
	}

	// Ensure ACK works: we don't get duplicated messages.

	{
		ctx, cancel := context.WithCancel(ctx)

		consume, err := Subscribe(ctx, drv)
		assert.NoError(t, err)

		assert.Error(t, context.Canceled, gock.Wait(func() error {
			return consume(ctx, func() (unwrapInto interface{}, handle HandleFunc) {
				messages := make([]string, 0, 3)
				return &messages, func(ctx context.Context) (context.Context, Ack) {
					got <- messages
					return ctx, <-ack
				}
			})
		}, func() error {
			defer cancel()

			chantest.AssertNoRecv(t, got)

			err := publishMessage(ctx, db, "msg6")
			assert.NoError(t, err)

			msgs := chantest.Before(1*time.Second).AssertRecv(t, got).([]string)
			assert.Equal(t, []string{"msg6"}, msgs)

			chantest.AssertSend(t, ack, OK)

			chantest.AssertNoRecv(t, got)

			return nil
		}))
		chantest.AssertNoRecv(t, got)
	}

	// Ensure requeue works.

	err = publishMessage(ctx, db, "msg6")
	assert.NoError(t, err)
	err = publishMessage(ctx, db, "msg7")
	assert.NoError(t, err)
	err = publishMessage(ctx, db, "msg8")
	assert.NoError(t, err)
	err = publishMessage(ctx, db, "msg9")
	assert.NoError(t, err)

	{
		ctx, cancel := context.WithCancel(ctx)

		consume, err := Subscribe(ctx, drv)
		assert.NoError(t, err)

		assert.Error(t, context.Canceled, gock.Wait(func() error {
			return consume(ctx, func() (unwrapInto interface{}, handle HandleFunc) {
				messages := make([]string, 0, 3)
				return &messages, func(ctx context.Context) (context.Context, Ack) {
					got <- messages
					return ctx, <-ack
				}
			})
		}, func() error {
			defer cancel()

			msgs := chantest.Before(1*time.Second).AssertRecv(t, got).([]string)
			assert.Equal(t, []string{"msg6", "msg7", "msg8"}, msgs)

			chantest.AssertSend(t, ack, Requeue)

			chantest.AssertNoRecv(t, got)

			return nil
		}))
		chantest.AssertNoRecv(t, got)
	}

	{
		ctx, cancel := context.WithCancel(ctx)

		consume, err := Subscribe(ctx, drv)
		assert.NoError(t, err)

		assert.Error(t, context.Canceled, gock.Wait(func() error {
			return consume(ctx, func() (unwrapInto interface{}, handle HandleFunc) {
				messages := make([]string, 0, 3)
				return &messages, func(ctx context.Context) (context.Context, Ack) {
					got <- messages
					return ctx, <-ack
				}
			})
		}, func() error {
			defer cancel()

			msgs := chantest.Before(1*time.Second).AssertRecv(t, got).([]string)
			assert.Equal(t, []string{"msg6", "msg7", "msg8"}, msgs)

			chantest.AssertSend(t, ack, OK)

			msgs = chantest.Before(1*time.Second).AssertRecv(t, got).([]string)
			assert.Equal(t, []string{"msg9"}, msgs)

			chantest.AssertSend(t, ack, OK)

			chantest.AssertNoRecv(t, got)

			return nil
		}))
		chantest.AssertNoRecv(t, got)
	}
}

func setupConsumersTest(t *testing.T, ctx context.Context, g gock.GoFunc, db sqlxTestDB, order OrderGuarantee) (consumers map[string]*testConsumer, cleanup func()) {
	// Concurrent consumers to test mutual exclusion.
	const concurrentConsumersPerSubscription = 2

	consumers = map[string]*testConsumer{}
	cleanup = func() {}

	for _, subscription := range []string{"foo", "bar"} {
		var subConsume []ConsumeFunc

		c := &testConsumer{
			subscription: subscription,
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
		consumers[subscription] = c

		for i := 0; i < concurrentConsumersPerSubscription; i++ {
			l := NewListener(db.connStr, time.Millisecond, time.Millisecond, nil)
			prevCleanup := cleanup
			cleanup = func() { l.Close(); prevCleanup() }

			consume, err := Subscribe(ctx, newTestPQSubscriptionDriver(db, l, subscription, order))
			assert.NoError(t, err)

			subConsume = append(subConsume, consume)
		}
	}

	for i := 0; i < 3; i++ {
		err := publishMessage(ctx, db, fmt.Sprintf("pending message %d", i))
		assert.NoError(t, err)
	}

	for _, c := range consumers {
		startTestConsumer(t, ctx, g, c)
	}

	return consumers, cleanup
}

func startTestConsumer(t *testing.T, ctx context.Context, g gock.GoFunc, c *testConsumer) {
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
			t.Errorf("had to cancel subscription %q", c.subscription)
			cancel()
		}

		select {
		case <-done:
		case <-time.After(1 * time.Second):
			t.Errorf("subscription %q consumer not killed by context cancel", c.subscription)
		}
	}
	c.kill = func() {
		t.Helper()
		cancel()

		select {
		case <-done:
		case <-time.After(1 * time.Second):
			t.Errorf("subscription %q consumer not killed by context cancel", c.subscription)
		}
	}

	g(func() error {
		defer close(done)
		err := c.consume(ctx, func() (unwrapInto interface{}, handle HandleFunc) {
			var payload string
			return &payload, func(ctx context.Context) (context.Context, Ack) {
				ack := make(chan Ack)
				c.deliveries <- testStringDelivered{payload, ack}
				return ctx, <-ack
			}
		})
		if errors.Is(err, context.Canceled) {
			err = nil
		}
		assert.NoError(t, err)
		return nil
	})
}

type receiveAssertions struct {
	receive     func(*testing.T)
	allReceived func(*testing.T)
}

func unorderedExpecter(c *testConsumer, expectedPayloads []string) receiveAssertions {
	expectedSet := map[string]struct{}{}
	for _, expected := range expectedPayloads {
		expectedSet[expected] = struct{}{}
	}

	deliveryCount := 0

	assertReceive := func(t *testing.T) {
		t.Helper()
		d := chantest.Before(1*time.Second).AssertRecv(t, c.deliveries, "subscription %q delivery %d: timeout waiting for delivery receive", c.subscription, deliveryCount).(testStringDelivered)
		delete(expectedSet, d.payload)
		chantest.AssertSend(t, d.ack, OK, "subscription %q delivery %d: timeout waiting for ack send", c.subscription, deliveryCount)
		deliveryCount++
	}

	assertAllReceived := func(t *testing.T) {
		t.Helper()
		assert.Len(t, expectedSet, 0)
	}

	return receiveAssertions{receive: assertReceive, allReceived: assertAllReceived}
}

type testStringDelivered struct {
	payload string
	ack     chan<- Ack
}

type testConsumer struct {
	subscription string
	consume      ConsumeFunc
	deliveries   chan testStringDelivered
	stop         func()
	kill         func()
}

// TODO: Exhaustive failure and concurrency tests.

type testPQSubscriptionDriver struct {
	db   sqler.DB
	l    *Listener
	name string
}

func newTestPQSubscriptionDriver(db sqler.DB, l *Listener, name string, ordered OrderGuarantee) PQSubscriptionDriver {
	drv := testPQSubscriptionDriver{
		db:   db,
		l:    l,
		name: name,
	}
	return PQSubscriptionDriver{
		DB:                           db,
		ExecInsertSubscription:       drv.ExecInsertSubscription,
		ListenForIncomingBaseQueries: drv.ListenForIncomingBaseQueries,
		PendingBaseQuery: NewQueryWithArgs(`
			SELECT serial, payload
			FROM message_deliveries m
			WHERE
				subscription = $1
			ORDER BY serial
		`, name),
		RowsToDeliveries: drv.RowsToDeliveries,
		Ordered:          ordered,
	}
}

func (drv testPQSubscriptionDriver) ExecInsertSubscription(ctx context.Context) error {
	_, err := drv.db.Exec(ctx, "INSERT INTO subscriptions (name) VALUES ($1);", drv.name)
	return ignoreUniqueViolation(err)
}

func (drv testPQSubscriptionDriver) ListenForIncomingBaseQueries(ctx context.Context) (AcceptQueriesFunc, func() error, error) {
	acceptNotifs, close, err := ListenForNotifications(ctx, drv.l, fmt.Sprintf("message:%s", drv.name))
	if err != nil {
		return nil, nil, fmt.Errorf("listening for notifications: %w", err)
	}
	return func(ctx context.Context, yield func(QueryWithArgs)) error {
		ctx, cancel := context.WithCancel(ctx)
		g, wait := gock.Bundle()
		notifs := NewPQNotificationIterator(g, func(yield func(*pq.Notification)) error {
			return acceptNotifs(ctx, yield)
		}, coro.KillOnContextDone(ctx))
		g(func() error {
			defer cancel()
			for notifs.Next() {
				yield(NewQueryWithArgs(`
					SELECT serial, payload
					FROM message_deliveries m
					WHERE
						subscription = $1
					ORDER BY serial
				`, drv.name))
			}
			return nil
		})
		return wait()
	}, close, nil
}

func (drv testPQSubscriptionDriver) RowsToDeliveries(ctx context.Context, deliveries *DeliveryRowsIterator) error {
	for deliveries.Next() {
		delivery := deliveries.Yielded

		var firstSerial int64
		var lastSerial int64

		delivery.Deliver(Delivery{
			Unwrap: func(into interface{}) error {
				var payload string

				switch into := into.(type) {
				case *string:
					delivery.Rows.Next()
					err := delivery.Rows.Scan(&lastSerial, &payload)
					if err != nil {
						return err
					}
					firstSerial = lastSerial
					*into = payload

				case *[]string:
					limit := -1
					if into != nil && *into != nil {
						limit = cap(*into)
						*into = (*into)[:0]
					}
					read := 0
					for read < limit && delivery.Rows.Next() {
						err := delivery.Rows.Scan(&lastSerial, &payload)
						if err != nil {
							return err
						}
						if read == 0 {
							firstSerial = lastSerial
						}
						*into = append(*into, payload)
						read++
					}

				default:
					panic(into)
				}

				delivery.Rows.Close()

				_, err := delivery.Exec(ctx, `
					UPDATE message_deliveries SET `+MarkAsDeliveredSQL+`
					WHERE
						serial >= $1 AND serial <= $2
						AND subscription = $3
				`, firstSerial, lastSerial, drv.name)
				if err != nil {
					return fmt.Errorf("marking as delivered: %w", err)
				}

				return nil
			},
			OK: func(ctx context.Context) error {
				_, err := delivery.Exec(ctx, `
					DELETE FROM message_deliveries
					WHERE
						serial >= $1 AND serial <= $2
						AND subscription = $3
				`, firstSerial, lastSerial, drv.name)
				return err
			},
			Requeue: func(ctx context.Context) error {
				_, err := delivery.Exec(ctx, `
					UPDATE message_deliveries SET `+UpdateLastAckSQL+`
					WHERE
						serial >= $1 AND serial <= $2
						AND subscription = $3
				`, firstSerial, lastSerial, drv.name)
				return err
			},
		})
	}
	return nil
}

var nextSerial int64

func publishMessage(ctx context.Context, db sqler.DB, payload string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}

	msgSerial := nextSerial
	nextSerial++

	_, err = tx.Exec(ctx, `
		DECLARE subscriptions_cursor CURSOR FOR
		SELECT name FROM subscriptions;
	`)
	if err != nil {
		return fmt.Errorf("selecting subscriptions: %w", err)
	}
	defer tx.Exec(ctx, `CLOSE subscriptions_cursor;`)

	for {
		var sub string
		err := tx.QueryRow(ctx, `FETCH NEXT FROM subscriptions_cursor;`).Scan(&sub)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				break
			}
			return fmt.Errorf("scanning subscription name: %w", err)
		}

		_, err = tx.Exec(ctx,
			"INSERT INTO message_deliveries (serial, subscription, payload) VALUES ($1, $2, $3);",
			msgSerial, sub, payload,
		)
		if ignoreUniqueViolation(err) != nil {
			return fmt.Errorf("inserting pending delivery for subscription %q and message %d: %w", sub, msgSerial, err)
		}
	}

	return tx.Commit()
}

func ignoreUniqueViolation(err error) error {
	var pqErr *pq.Error
	const uniqueViolation = "23505"
	if errors.As(err, &pqErr) && pqErr.Code == uniqueViolation {
		err = nil
	}
	return err
}

type sqlxTestDB struct {
	sqler.DB
	connStr string
}

func createTestDB(ctx context.Context, t *testing.T) (sqlxTestDB, func()) {
	name := fmt.Sprintf("pgqueue_test_%s", strings.ToLower(ulidx.New()))

	db, err := sqler.Open("postgres", fmt.Sprintf(postgresConnString, baseDB))
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
	testDB, err := sqler.Open("postgres", connStr)
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

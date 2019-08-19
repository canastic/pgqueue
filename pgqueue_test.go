package pgqueue

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.com/canastic/chantest"
	"golang.org/x/xerrors"
)

func TestSubscribe(t *testing.T) {
	drivers := map[string]*testSubscriptionDriver{}
	consumers := map[string]func(context.Context, GetHandler) error{}

	for _, sub := range []string{"subA", "subB"} {
		d := newTestSubscriptionDriver()
		drivers[sub] = d

		go func() {
			chantest.AssertRecv(t, d.insertSubscriptionCalls).(chan<- error) <- nil
		}()

		var err error
		consumers[sub], err = Subscribe(d)
		assert.Nil(t, err)
	}

	type opSource string
	const (
		opSourceIncoming = "incoming"
		opSourcePending  = "pending"
	)
	type op struct {
		sub    string
		source opSource
		ack    Ack
		msg    string
	}

	ops := []*op{
		{"subA", opSourceIncoming, OK, ""},
		{"subA", opSourcePending, Requeue, ""},
		{"subB", opSourcePending, Requeue, ""},
		{"subB", opSourceIncoming, OK, ""},
		{"subB", opSourceIncoming, OK, ""},
		{"subA", opSourcePending, Requeue, ""},
		{"subB", opSourceIncoming, Requeue, ""},
		{"subB", opSourcePending, OK, ""},
	}

	for i, op := range ops {
		op.msg = fmt.Sprintf("msg %d -> %s.%s (%v)", i, op.sub, op.source, op.ack)

		var src chan msgWithAck

		switch op.source {
		case opSourceIncoming:
			src = drivers[op.sub].incoming
		case opSourcePending:
			src = drivers[op.sub].pending
		default:
			panic(op.source)
		}

		src <- msgWithAck{op.msg, op.ack}
	}

	for _, sub := range []string{"subA", "subB"} {
		handled := make(chan testHandled)

		stopConsumer := start(context.Background(), func(ctx context.Context) {
			consumers[sub](ctx, func() (unwrapInto interface{}, handle func() Ack) {
				var msg fakeMessage
				return &msg, func() Ack {
					ack := make(chan Ack)
					handled <- testHandled{msg.payload, ack}
					return <-ack
				}
			})
		})

		doOp := func(op *op) {
			h := chantest.AssertRecv(t, handled).(testHandled)
			assert.Equal(t, op.msg, h.msg)
			chantest.AssertSend(t, h.ack, op.ack)

			acked := chantest.AssertRecv(t, drivers[sub].acks).(msgWithAck)
			assert.Equal(t, h.msg, acked.msg)
			assert.Equal(t, op.ack, acked.ack)
		}

		for _, op := range ops {
			if op.sub == sub && op.source == opSourcePending {
				doOp(op)
			}
		}

		close(drivers[sub].pending)

		for _, op := range ops {
			if op.sub == sub && op.source == opSourceIncoming {
				doOp(op)
			}
		}

		chantest.Expect(t, stopConsumer)
	}
}

func TestRequeueOnCrash(t *testing.T) {
	driver := newTestSubscriptionDriver()

	go func() {
		chantest.AssertRecv(t, driver.insertSubscriptionCalls).(chan<- error) <- nil
	}()

	consumer, err := Subscribe(driver)
	assert.Nil(t, err)

	stopConsumer := start(context.Background(), func(ctx context.Context) {
		err := consumer(ctx, func() (unwrapInto interface{}, handle func() Ack) {
			panic("oops")
		})
		assert.NotNil(t, err)
		_, ok := err.(Panic)
		assert.True(t, ok)
	})

	chantest.AssertSend(t, driver.pending, msgWithAck{})

	assert.Equal(t, Requeue, chantest.AssertRecv(t, driver.acks).(msgWithAck).ack)

	chantest.Expect(t, stopConsumer)
}

func TestListenBeforeFetchingPending(t *testing.T) {
	listenShouldReturn := make(chan struct{})
	fetchCalled := make(chan struct{})

	m := (&SubscriptionDriverMocker{}).Describe()

	m = m.InsertSubscription().Returns(nil).Times(1)

	m = m.ListenForDeliveries().TakesAny().ReturnsFrom(func(context.Context) (func(context.Context, chan<- Delivery) error, error) {
		<-listenShouldReturn
		return func(ctx context.Context, _ chan<- Delivery) error {
			<-ctx.Done()
			return nil
		}, nil
	}).Times(1)

	m = m.FetchPendingDeliveries().TakesAny().AndAny().ReturnsFrom(func(context.Context, chan<- Delivery) error {
		fetchCalled <- struct{}{}
		return nil
	}).Times(1)

	driver, assertMock := m.Mock()
	defer assertMock(t)

	consume, err := Subscribe(driver)
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	stopConsuming := start(ctx, func(ctx context.Context) {
		consume(ctx, func() (unwrapInto interface{}, handle func() Ack) {
			panic("unexpected")
		})
	})

	chantest.AssertNoRecv(t, fetchCalled)

	listenShouldReturn <- struct{}{}
	chantest.AssertRecv(t, fetchCalled)

	cancel()
	chantest.Expect(t, stopConsuming)
}

func TestErrorOnInsert(t *testing.T) {
	expectedErr := errors.New("oops")

	driver, assertMock := (&SubscriptionDriverMocker{}).Describe().
		InsertSubscription().Returns(expectedErr).Times(1).
		Mock()
	defer assertMock(t)

	_, err := Subscribe(driver)
	assert.True(t, xerrors.Is(err, expectedErr), err)
}

func TestErrorOnListen(t *testing.T) {
	expectedErr := errors.New("oops")

	driver, assertMock := (&SubscriptionDriverMocker{}).Describe().
		InsertSubscription().Returns(nil).Times(1).
		ListenForDeliveries().TakesAny().Returns(nil, expectedErr).Times(1).
		Mock()
	defer assertMock(t)

	consume, err := Subscribe(driver)
	assert.NoError(t, err)
	err = consume(context.Background(), nil)
	assert.True(t, xerrors.Is(err, expectedErr), err)
}

func TestErrorOnFetchPending(t *testing.T) {
	expectedErr := errors.New("oops")

	m := (&SubscriptionDriverMocker{}).Describe().
		InsertSubscription().Returns(nil).Times(1)

	m = m.ListenForDeliveries().TakesAny().Returns(func(ctx context.Context, _ chan<- Delivery) error {
		<-ctx.Done()
		return nil
	}, nil).Times(1)

	m = m.FetchPendingDeliveries().TakesAny().AndAny().Returns(expectedErr).Times(1)

	driver, assertMock := m.Mock()
	defer assertMock(t)

	consume, err := Subscribe(driver)
	assert.NoError(t, err)
	err = consume(context.Background(), nil)
	assert.True(t, xerrors.Is(err, expectedErr), err)
}

func TestErrorOnAccept(t *testing.T) {
	expectedErr := errors.New("oops")

	m := (&SubscriptionDriverMocker{}).Describe().
		InsertSubscription().Returns(nil).Times(1)

	m = m.ListenForDeliveries().TakesAny().Returns(func(ctx context.Context, _ chan<- Delivery) error {
		return expectedErr
	}, nil).Times(1)

	m = m.FetchPendingDeliveries().TakesAny().AndAny().Returns(nil).Times(1)

	driver, assertMock := m.Mock()
	defer assertMock(t)

	consume, err := Subscribe(driver)
	assert.NoError(t, err)
	err = consume(context.Background(), nil)
	assert.True(t, xerrors.Is(err, expectedErr), err)
}

func TestErrorOnUnwrap(t *testing.T) {
	expectedErr := errors.New("oops")

	m := (&SubscriptionDriverMocker{}).Describe().
		InsertSubscription().Returns(nil).Times(1)

	m = m.ListenForDeliveries().TakesAny().Returns(func(ctx context.Context, deliveries chan<- Delivery) error {
		delivery, assertMock := (&DeliveryMocker{}).Describe().
			UnwrapMessage().TakesAny().Returns(expectedErr).Times(1).
			Ack().TakesAny().Times(1).
			Mock()
		defer assertMock(t)

		deliveries <- delivery

		<-ctx.Done()
		return nil
	}, nil).Times(1)

	m = m.FetchPendingDeliveries().TakesAny().AndAny().Returns(nil).Times(1)

	driver, assertMock := m.Mock()
	defer assertMock(t)

	consume, err := Subscribe(driver)
	assert.NoError(t, err)
	err = consume(context.Background(), func() (unwrapInto interface{}, handle func() Ack) {
		return nil, nil
	})
	assert.True(t, xerrors.Is(err, expectedErr), err)
}

type fakeMessage struct {
	payload string
}

type msgWithAck struct {
	msg string
	ack Ack
}

type testHandled struct {
	msg string
	ack chan<- Ack
}

type testSubscriptionDriver struct {
	acks     chan msgWithAck
	incoming chan msgWithAck
	pending  chan msgWithAck

	insertSubscriptionCalls chan (chan<- error)
}

func newTestSubscriptionDriver() *testSubscriptionDriver {
	return &testSubscriptionDriver{
		acks:                    make(chan msgWithAck),
		incoming:                make(chan msgWithAck, 9999),
		pending:                 make(chan msgWithAck, 9999),
		insertSubscriptionCalls: make(chan (chan<- error), 1),
	}
}

func (d *testSubscriptionDriver) InsertSubscription() error {
	ch := make(chan error)
	d.insertSubscriptionCalls <- ch
	return <-ch
}

func (d *testSubscriptionDriver) FetchPendingDeliveries(ctx context.Context, deliveries chan<- Delivery) error {
	return d.forward(ctx, deliveries, d.pending)
}

func (d *testSubscriptionDriver) ListenForDeliveries(ctx context.Context) (func(context.Context, chan<- Delivery) error, error) {
	return func(ctx context.Context, deliveries chan<- Delivery) error {
		return d.forward(ctx, deliveries, d.incoming)
	}, nil
}

func (d *testSubscriptionDriver) forward(ctx context.Context, into chan<- Delivery, from <-chan msgWithAck) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case del, ok := <-from:
			if !ok {
				return nil
			}
			into <- testDelivery{del.msg, d.acks}
		}
	}
}

type testDelivery struct {
	msg   string
	onAck chan<- msgWithAck
}

func (d testDelivery) UnwrapMessage(into interface{}) error {
	into.(*fakeMessage).payload = d.msg
	return nil
}

func (d testDelivery) Ack(ack Ack) {
	d.onAck <- msgWithAck{d.msg, ack}
}

func start(ctx context.Context, f func(context.Context)) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		f(ctx)
	}()
	return func() {
		cancel()
		<-done
	}
}

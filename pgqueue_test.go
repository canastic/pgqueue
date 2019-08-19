package pgqueue

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.com/canastic/chantest"
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
	defer close(into)
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

func (d testDelivery) Ack(ack Ack) error {
	d.onAck <- msgWithAck{d.msg, ack}
	return nil
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

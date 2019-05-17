package pgqueue

import (
	"context"
	"fmt"
	"runtime/debug"
)

func Subscribe(driver SubscriptionDriver) (consume func(context.Context, GetHandler) error, err error) {
	err = driver.InsertSubscription()
	if err != nil {
		return nil, err
	}

	return func(ctx context.Context, getHandler GetHandler) error {
		// Ensure we cancel the asynchronous ListenForDeliveries call on panic.
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		consume := func(deliveries <-chan Delivery) error {
			for d := range deliveries {
				handleDelivery(d, getHandler)
			}
			return nil
		}

		asyncErr := make(chan error, 1)

		incoming := make(chan Delivery)
		pending := make(chan Delivery)

		goRecovering(asyncErr, func() error {
			return driver.ListenForDeliveries(ctx, incoming)
		})

		goRecovering(asyncErr, func() error {
			return consume(pending)
		})
		err := driver.FetchPendingDeliveries(ctx, pending)
		if err := <-asyncErr; err != nil {
			return err
		}

		err = consume(incoming)
		if err != nil {
			return err
		}
		return <-asyncErr
	}, nil
}

func handleDelivery(d Delivery, getHandler GetHandler) error {
	ack := Requeue
	defer func() {
		_ = d.Ack(ack)
	}()

	into, handle := getHandler()

	err := d.UnwrapMessage(into)
	if err != nil {
		return err
	}

	ack = handle()
	return nil
}

type GetHandler func() (unwrapInto interface{}, handle func() Ack)

type SubscriptionDriver interface {
	InsertSubscription() error
	ListenForDeliveries(context.Context, chan<- Delivery) error
	FetchPendingDeliveries(context.Context, chan<- Delivery) error
}

type Delivery interface {
	UnwrapMessage(into interface{}) error
	Ack(Ack) error
}

type Ack bool

const (
	OK      Ack = true
	Requeue Ack = false
)

func (ack Ack) String() string {
	switch ack {
	case OK:
		return "OK"
	case Requeue:
		return "Requeue"
	default:
		panic(fmt.Errorf("unknown value for Ack: %v", bool(ack)))
	}
}

type Panic struct {
	p     interface{}
	stack []byte
}

func (p Panic) Error() string {
	return fmt.Sprintf("%v\n\n%s", p.p, p.stack)
}

func goRecovering(errCh chan<- error, f func() error) {
	go func() {
		var err error
		defer func() {
			if r := recover(); r != nil {
				err = Panic{r, debug.Stack()}
			}
			errCh <- err
		}()
		err = f()
	}()
}

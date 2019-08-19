// Package pgqueue implements a durable, at-least-once, optionally ordered
// message queue on top of PostgreSQL.
package pgqueue

import (
	"context"
	"fmt"
	"runtime/debug"

	"github.com/tcard/gock"
	"golang.org/x/xerrors"
)

// Subscribe creates a subscription and returns a function to consume from it.
//
// A published message will be copied to all existing subscriptions at the time,
// even if they aren't any active consumers from it.
//
// It depends on the provided SubscriptionDriver how message delivery for
// concurrent consumers to the same subscription behaves.
func Subscribe(driver SubscriptionDriver) (consume func(context.Context, GetHandler) error, err error) {
	err = driver.InsertSubscription()
	if err != nil {
		return nil, err
	}

	return func(ctx context.Context, getHandler GetHandler) (err error) {
		defer catchPanic(&err)

		acceptIncoming, err := driver.ListenForDeliveries(ctx)
		if err != nil {
			return xerrors.Errorf("listening for deliveries: %w", err)
		}

		deliveries := make(chan Delivery)

		ctx, cancel := context.WithCancel(ctx)

		return gock.Wait(func() error {
			defer cancel()
			for d := range deliveries {
				err := handleDelivery(d, getHandler)
				if err != nil {
					return xerrors.Errorf("handling delivery: %w", err)
				}
			}
			return nil
		}, func() error {
			defer close(deliveries)

			err := driver.FetchPendingDeliveries(ctx, deliveries)
			if err != nil {
				return xerrors.Errorf("fetching pending deliveries: %w", err)
			}

			err = acceptIncoming(ctx, deliveries)
			if err != nil {
				return xerrors.Errorf("accepting incoming deliveries: %w", err)
			}

			return nil
		})
	}, nil
}

func handleDelivery(d Delivery, getHandler GetHandler) error {
	ack := Requeue
	defer func() {
		d.Ack(ack)
	}()

	into, handle := getHandler()

	err := d.UnwrapMessage(into)
	if err != nil {
		return err
	}

	ack = handle()
	return nil
}

// GetHandler is a function that is called with each incoming message. The
// function provides a value to unwrap the message into, and a handler function
// to then use this value.
//
// When a message arrives as a Delivery from the ListenForDeliveries or
// FetchPendingDeliveries methods of the SubscriptionDriver, this function,
// provided to the consume function returned by Subscriber, is called. Then,
// the Delivery's UnwrapMessage is called with the returned unwrapInto value.
// If that doesn't fail, the handle function is called.
//
// The handle function should return OK to acknowledge that the message has been
// processed and should be removed from the queue, or Requeue otherwise.
type GetHandler func() (unwrapInto interface{}, handle func() Ack)

//go:generate make.go.mock -type SubscriptionDriver

// A SubscriptionDriver is the abstract interface that
type SubscriptionDriver interface {
	InsertSubscription() error
	ListenForDeliveries(context.Context) (AcceptFunc, error)
	FetchPendingDeliveries(context.Context, chan<- Delivery) error
}

type AcceptFunc func(context.Context, chan<- Delivery) error

//go:generate make.go.mock -type Delivery

// A Delivery is an attempted delivery of a message.
type Delivery interface {
	// UnwrapMessage unwraps the message as it comes from the queue into a value
	// that a handler can use.
	UnwrapMessage(into interface{}) error
	// Ack should remove the message from the queue if it's OK, or release it
	// to be delivered again later if it's not.
	//
	// Ack is best-effort; the library's protocol doesn't care about it failing.
	Ack(Ack)
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

// A Panic is a panic captured as an error. Is is returned by the consume
// function returned by Subscribe when either the deliveries listener or the
// provided handler panic.
type Panic struct {
	p     interface{}
	stack []byte
}

func (p Panic) Error() string {
	return fmt.Sprintf("%v\n\n%s", p.p, p.stack)
}

func (p Panic) Unwrap() error {
	switch err := p.p.(type) {
	case error:
		return err
	default:
		return nil
	}
}

func catchPanic(err *error) {
	if r := recover(); r != nil {
		*err = Panic{r, debug.Stack()}
	}
}

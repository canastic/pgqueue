// Package pgqueue implements a durable, at-least-once, optionally ordered
// message queue on top of PostgreSQL.
package pgqueue

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"

	"github.com/canastic/pgqueue/stopcontext"
	"github.com/tcard/coro"
	"github.com/tcard/gock"
)

// Subscribe creates a subscription and returns a function to consume from it.
//
// A published message will be copied to all existing subscriptions at the time,
// even if they aren't any active consumers from it.
//
// It depends on the provided SubscriptionDriver how message delivery for
// concurrent consumers to the same subscription behaves.
func Subscribe(ctx context.Context, driver SubscriptionDriver) (consume ConsumeFunc, err error) {
	err = driver.InsertSubscription(ctx)
	if err != nil {
		return nil, err
	}

	return func(ctx context.Context, getHandler GetHandler) (err error) {
		acceptIncoming, closeListener, err := driver.ListenForDeliveries(ctx)
		if err != nil {
			return fmt.Errorf("listening for deliveries: %w", err)
		}
		defer func() {
			err = gock.AddConcurrentError(err, closeListener())
		}()

		g, wait := gock.Bundle()
		defer wait()

		g = goPanicAsError(g)

		coroCtx, cancel := context.WithCancel(ctx)

		var deliveries *DeliveryIterator
		deliveries = NewDeliveryIterator(g, func(yield func(Delivery)) error {
			err := driver.FetchPendingDeliveries(ctx, yield)
			if err != nil {
				return fmt.Errorf("fetching pending deliveries: %w", err)
			}

			err = acceptIncoming(ctx, yield)
			if err != nil {
				return fmt.Errorf("accepting incoming deliveries: %w", err)
			}

			return nil
		}, coro.KillOnContextDone(coroCtx))

		g(func() error {
			defer cancel()
			for {
				// Prioritize the stop signal, so that the client can be
				// guaranteed that the handler won't be called anymore by
				// stopping the context before ACKing the last delivery
				// received.
				select {
				case <-stopcontext.Stopped(ctx):
					return ctx.Err()
				default:
				}

				if !deliveries.Next() {
					return nil
				}

				// If the context is cancelled while the handler is running,
				// we just ignore the handler and kill everything on our side.
				// This is a more fail-fast approach, and there's stopcontext
				// if consumers want a softer stop.

				handleErr := make(chan error, 1)
				go func() {
					handleErr <- func() (err error) {
						defer panicAsError(&err)
						return handleDelivery(ctx, deliveries.Yielded, getHandler)
					}()
				}()

				var err error
				select {
				case <-ctx.Done():
					err = ctx.Err()
				case err = <-handleErr:
				}
				if err != nil {
					return fmt.Errorf("handling delivery: %w", err)
				}
			}
		})

		return wait()
	}, nil
}

type ConsumeFunc = func(context.Context, GetHandler) error

var ErrRequeued = errors.New("a message was requeued; redelivery not guaranteed unless consuming starts again")

func handleDelivery(ctx context.Context, d Delivery, getHandler GetHandler) (err error) {
	ack := Requeue

	defer func() {
		ackErr := d.Ack(ctx, ack)

		if ackErr == nil && ack == Requeue {
			ackErr = ErrRequeued
		}

		// If there was an error already, ack is best-effort. The important
		// thing here is that we return some error so the ConsumeFunc stops.
		if err == nil {
			err = ackErr
		}
	}()

	into, handle := getHandler()

	err = d.Unwrap(into)
	if err != nil {
		return err
	}

	ctx, ack = handle(ctx)
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
type GetHandler = func() (unwrapInto interface{}, handle HandleFunc)

type HandleFunc = func(context.Context) (context.Context, Ack)

//go:generate make.go.mock -type SubscriptionDriver

// A SubscriptionDriver is the abstract interface that
type SubscriptionDriver interface {
	InsertSubscription(context.Context) error
	ListenForDeliveries(context.Context) (accept AcceptFunc, close func() error, err error)
	FetchPendingDeliveries(context.Context, func(Delivery)) error
}

type AcceptFunc = func(context.Context, func(Delivery)) error

// A Delivery is an attempted delivery of a message.
type Delivery struct {
	// Unwrap unwraps the delivery as it comes from the queue into a value
	// that a handler can use.
	Unwrap  func(into interface{}) error
	OK      func(context.Context) error
	Requeue func(context.Context) error
}

func (d Delivery) Ack(ctx context.Context, ack Ack) error {
	switch ack {
	case OK:
		err := d.OK(ctx)
		if err != nil {
			return fmt.Errorf("acknowledging delivery: %w", err)
		}
		return nil
	case Requeue:
		err := d.Requeue(ctx)
		if err != nil {
			return fmt.Errorf("requeuing delivery: %w", err)
		}
		return nil
	default:
		panic(fmt.Errorf("unknown value for Ack: %v", bool(ack)))
	}
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

func goPanicAsError(g func(func() error)) func(func() error) {
	return func(f func() error) {
		g(func() (err error) {
			defer panicAsError(&err)
			return f()
		})
	}
}

func panicAsError(err *error) {
	if r := recover(); r != nil {
		*err = Panic{r, debug.Stack()}
	}
}

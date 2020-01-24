package pgqueue

import (
	"github.com/lib/pq"
	"github.com/tcard/coro"
)

func goError(g func(func() error), returned *error) coro.GoFunc {
	return func(f func()) {
		g(func() error {
			f()
			return *returned
		})
	}
}

func NewDeliveryIterator(g func(func() error), f func(yield func(Delivery)) error, options ...coro.SetOption) *DeliveryIterator {
	var it DeliveryIterator
	it.Next = coro.New(
		func(yield func()) {
			it.Returned = f(func(v Delivery) {
				it.Yielded = v
				yield()
			})
		},
		append(options,
			coro.WithGoFunc(goError(g, &it.Returned)),
		)...,
	)
	return &it
}

type DeliveryIterator struct {
	Next     coro.Resume
	Yielded  Delivery
	Returned error
}

func NewDeliveryRowIterator(g func(func() error), f func(yield func(DeliveryRow)) error, options ...coro.SetOption) *DeliveryRowIterator {
	var it DeliveryRowIterator
	it.Next = coro.New(
		func(yield func()) {
			it.Returned = f(func(v DeliveryRow) {
				it.Yielded = v
				yield()
			})
		},
		append(options,
			coro.WithGoFunc(goError(g, &it.Returned)),
		)...,
	)
	return &it
}

type DeliveryRowIterator struct {
	Next     coro.Resume
	Yielded  DeliveryRow
	Returned error
}

func NewQueryWithArgsIterator(g func(func() error), f func(yield func(QueryWithArgs)) error, options ...coro.SetOption) *QueryWithArgsIterator {
	var it QueryWithArgsIterator
	it.Next = coro.New(
		func(yield func()) {
			it.Returned = f(func(v QueryWithArgs) {
				it.Yielded = v
				yield()
			})
		},
		append(options,
			coro.WithGoFunc(goError(g, &it.Returned)),
		)...,
	)
	return &it
}

type QueryWithArgsIterator struct {
	Next     coro.Resume
	Yielded  QueryWithArgs
	Returned error
}

func NewPQNotificationIterator(g func(func() error), f func(yield func(*pq.Notification)) error, options ...coro.SetOption) *PQNotificationIterator {
	var it PQNotificationIterator
	it.Next = coro.New(
		func(yield func()) {
			it.Returned = f(func(v *pq.Notification) {
				it.Yielded = v
				yield()
			})
		},
		append(options,
			coro.WithGoFunc(goError(g, &it.Returned)),
		)...,
	)
	return &it
}

type PQNotificationIterator struct {
	Next     coro.Resume
	Yielded  *pq.Notification
	Returned error
}

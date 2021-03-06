package pgqueue

import (
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

func NewDeliveryRowsIterator(g func(func() error), f func(yield func(DeliveryRows)) error, options ...coro.SetOption) *DeliveryRowsIterator {
	var it DeliveryRowsIterator
	it.Next = coro.New(
		func(yield func()) {
			it.Returned = f(func(v DeliveryRows) {
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

type DeliveryRowsIterator struct {
	Next     coro.Resume
	Yielded  DeliveryRows
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

package exampleiterator

import (
	"gitlab.com/canastic/pgqueue/coro"
)

type Foo string

func NewFooIterator(f func(yield func(Foo)) error, options ...coro.SetOption) *FooIterator {
	var it FooIterator
	it.Next = coro.New(func(yield func()) {
		it.Returned = f(func(v Foo) {
			it.Yielded = v
			yield()
		})
	}, options...)
	return &it
}

type FooIterator struct {
	Next     coro.Resume
	Yielded  Foo
	Returned error
}

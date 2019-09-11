package coro

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
)

type GoFunc func(func())

type Resume = func() (returned bool)
type Kill = func()

type Options struct {
	g       GoFunc
	killCtx context.Context
}

type SetOption func(*Options)

func KillOnContextDone(ctx context.Context) SetOption {
	return func(o *Options) {
		o.killCtx = ctx
	}
}

func WithGoFunc(g GoFunc) SetOption {
	return func(o *Options) {
		o.g = g
	}
}

var defaultOptions = []SetOption{
	KillOnContextDone(context.Background()),
	WithGoFunc(func(f func()) { go f() }),
}

func New(f func(yield func()), setOptions ...SetOption) Resume {
	var options Options
	for _, setOption := range append(defaultOptions, setOptions...) {
		setOption(&options)
	}

	yieldCh := make(chan struct{})
	garbageCollected := make(chan struct{})

	resume := func() bool {
		// resume...
		_, ok := <-yieldCh
		if !ok {
			// resumed dead coroutine
			return false
		}

		// ... and wait for suspend or return
		_, ok = <-yieldCh
		return ok
	}

	runtime.SetFinalizer(&resume, func(interface{}) {
		close(garbageCollected)
	})

	waitResume := func() {
		select {
		case yieldCh <- struct{}{}:
		case <-garbageCollected:
			panic(ErrLeak)
		case <-options.killCtx.Done():
			panic(ErrKilled{options.killCtx.Err()})
		}
	}

	options.g(func() {
		defer close(yieldCh)

		defer func() {
			r := recover()
			if r == nil {
				return
			}
			if err, ok := r.(error); ok && errors.As(err, &ErrKilled{}) {
				return
			}
			panic(r)
		}()

		waitResume()

		f(func() {
			// make call to Resume return
			yieldCh <- struct{}{}

			waitResume()
		})
	})

	return resume
}

var ErrLeak = errors.New("coro: coroutine leaked; Resume function garbage collected before coroutine returned")

type ErrKilled struct {
	By error
}

func (err ErrKilled) Error() string {
	return fmt.Errorf("coro: coroutine killed by context done: %w", err.By).Error()
}

func (err ErrKilled) Unwrap() error {
	return err.By
}

func NewIterator(yielded, returned interface{}, f func(yield func(interface{})) interface{}, setOption ...SetOption) Resume {
	setYielded := reflect.ValueOf(yielded).Elem().Set
	setReturned := reflect.ValueOf(returned).Elem().Set
	return New(func(yield func()) {
		returned := f(func(v interface{}) {
			setYielded(reflect.ValueOf(v))
			yield()
		})
		setReturned(reflect.ValueOf(returned))
	}, setOption...)
}

package coro_test

import (
	"errors"
	"fmt"
	"runtime"
	"testing"

	"gitlab.com/canastic/pgqueue/coro"
)

func ExampleNew() {
	resume := coro.New(func(yield func()) {
		for i := 1; i <= 3; i++ {
			fmt.Println("coroutine:", i)
			yield()
		}
		fmt.Println("coroutine: done")
	})

	fmt.Println("not started yet")
	for resume() {
		fmt.Println("yielded")
	}
	fmt.Println("returned")

	// Output:
	// not started yet
	// coroutine: 1
	// yielded
	// coroutine: 2
	// yielded
	// coroutine: 3
	// yielded
	// coroutine: done
	// returned
}

func ExampleNewIterator() {
	var yielded int
	var returned error
	next := coro.NewIterator(&yielded, &returned, func(yield func(interface{})) interface{} {
		for i := 1; i <= 3; i++ {
			yield(i)
		}
		return errors.New("done")
	})

	for next() {
		fmt.Println("yielded:", yielded)
	}
	fmt.Println("returned:", returned)

	// Output:
	// yielded: 1
	// yielded: 2
	// yielded: 3
	// returned: done
}

func TestLeak(t *testing.T) {
	panicked := make(chan interface{})
	func() {
		resume := coro.New(func(yield func()) {
			defer func() {
				if r := recover(); r != nil {
					panicked <- r
				}
			}()
			yield()
		})
		resume()
	}()
	runtime.GC()
	p := <-panicked
	if err, ok := p.(error); !ok || !errors.As(err, &coro.ErrKilled{}) || !errors.Is(err, coro.ErrLeak) {
		t.Errorf("expected ErrLeak within an ErrKilled, got %v", p)
	}
}

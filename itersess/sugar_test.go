package itersess

import (
	"context"
	"sync"
	"testing"
	"time"

	"runtime/debug"

	"github.com/stretchr/testify/assert"
)

func TestBasic(t *testing.T) {
	defer assertNoGoroutineLeaks(t)()

	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(2)
	giver, puller := New()

	toGive := []string{"foo", "bar", "qux"}

	var shared string

	go func() {
		defer wg.Done()

		_, waitPull, done := giver.Loop(ctx)
		defer done()

		i := 0
		for waitPull() {
			if i >= len(toGive) {
				break
			}
			shared = toGive[i]
			i++
		}
	}()

	var got []string

	go func() {
		defer wg.Done()
		pull, done := puller.Loop(ctx)
		defer done()

		for pull() {
			got = append(got, shared)
		}
	}()

	wg.Wait()

	assert.Equal(t, toGive, got)
}

func TestCancelPullBeforeAccepted(t *testing.T) {
	defer assertNoGoroutineLeaks(t)

	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(2)
	giver, puller := New()

	go func() {
		defer wg.Done()
		_, waitPull, done := giver.Loop(ctx)
		defer done()

		assert.False(t, waitPull())
	}()

	pullCtx, cancel := context.WithCancel(ctx)
	cancel()
	go func() {
		defer wg.Done()
		pull, done := puller.Loop(pullCtx)
		defer done()

		assert.False(t, pull())
	}()

	wg.Wait()
}

func TestCancelPullAfterAccepted(t *testing.T) {
	defer assertNoGoroutineLeaks(t)()

	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(2)
	giver, puller := New()

	pullCtx, cancel := context.WithCancel(ctx)

	go func() {
		defer wg.Done()
		ctx, waitPull, done := giver.Loop(ctx)
		defer done()

		assert.True(t, waitPull())
		cancel()
		<-ctx.Done()
	}()

	go func() {
		defer wg.Done()
		pull, done := puller.Loop(pullCtx)
		defer done()

		assert.False(t, pull())
	}()

	wg.Wait()
}

func TestReturnEarlyFromPullerLoop(t *testing.T) {
	defer assertNoGoroutineLeaks(t)()

	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(2)
	giver, puller := New()

	go func() {
		defer wg.Done()
		_, waitPull, done := giver.Loop(ctx)
		defer done()

		pulls := 0
		for waitPull() {
			pulls++
		}
		assert.Equal(t, 3, pulls)
	}()

	go func() {
		defer wg.Done()
		pull, done := puller.Loop(ctx)
		defer done()

		for i := 0; i < 3; i++ {
			assert.True(t, pull())
		}
	}()

	wg.Wait()
}

func TestReturnEarlyFromGiverLoop(t *testing.T) {
	defer assertNoGoroutineLeaks(t)()

	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(2)
	giver, puller := New()

	go func() {
		defer wg.Done()
		_, waitPull, done := giver.Loop(ctx)
		defer done()

		pulls := 0
		for waitPull() && pulls < 3 {
			pulls++
		}
	}()

	go func() {
		defer wg.Done()
		pull, done := puller.Loop(ctx)
		defer done()

		pulls := 0
		for pull() {
			pulls++
		}
		assert.Equal(t, 3, pulls)
	}()

	wg.Wait()
}

func assertNoGoroutineLeaks(t *testing.T) func() {
	var stacksMtx sync.Mutex
	var nextStack int
	stacks := map[int][]byte{}

	var wg sync.WaitGroup

	oldGoo := goo
	goo = func(f func()) {
		stacksMtx.Lock()
		defer stacksMtx.Unlock()

		myStack := nextStack
		nextStack++
		stacks[myStack] = debug.Stack()

		wg.Add(1)

		go func() {
			defer wg.Done()
			f()

			stacksMtx.Lock()
			defer stacksMtx.Unlock()
			delete(stacks, myStack)
		}()
	}

	return func() {
		waited := make(chan struct{})
		go func() {
			defer close(waited)
			wg.Wait()
		}()

		select {
		case <-waited:
		case <-time.After(50 * time.Millisecond):
		}

		goo = oldGoo

		stacksMtx.Lock()
		defer stacksMtx.Unlock()

		for _, stack := range stacks {
			t.Errorf("goroutine leaked: %s", stack)
		}
	}
}

package cspiter_test

import (
	"errors"
	"fmt"

	"github.com/tcard/gock"
	"gitlab.com/canastic/pgqueue/cspiter"
)

type Foo string

func NewFooIterator(last *Foo) (FooSends, FooReceives) {
	sends, receives := cspiter.NewIterator()
	return FooSends{Sends: sends, Last: last}, FooReceives{Receives: receives, Last: last}
}

type FooSends struct {
	cspiter.Sends
	Last *Foo
}

func (s FooSends) TrySend(v Foo) (sent bool) {
	send, ok := s.WaitPull()
	if ok {
		send(v)
	}
	return ok
}

func (s FooSends) WaitPull() (send func(Foo), ok bool) {
	_, ok = <-s.Pulls
	if !ok {
		return nil, false
	}
	return func(v Foo) { *s.Last = v; s.Ready <- true }, true
}

type FooReceives struct {
	cspiter.Receives
	Last *Foo
}

func (r FooReceives) Got() Foo {
	return *r.Last
}

func Example() {
	var foo Foo
	sends, receives := NewFooIterator(&foo)

	err := gock.Wait(func() error {
		defer sends.Done()

		sends.TrySend("first")
		sends.TrySend("then")

		err := sendMoreFoos(sends)
		if err != nil {
			return err
		}

		return sendFoosUntilError(sends)
	}, func() error {
		defer receives.Done()

		for receives.Pull() {
			fmt.Println("got:", foo)
		}

		return nil
	})

	fmt.Println(err)

	// Output:
	// got: first
	// got: then
	// got: more
	// got: another
	// got: foo 1
	// got: foo 2
	// got: foo 3
	// no more sends
}

func sendMoreFoos(foos FooSends) error {
	foos.TrySend("more")
	foos.TrySend("another")
	return nil
}

func sendFoosUntilError(foos FooSends) error {
	i := 0
	for send, ok := foos.WaitPull(); ok; send, ok = foos.WaitPull() {
		i++
		if i > 3 {
			break
		}

		send(Foo(fmt.Sprintf("foo %d", i)))
	}
	return errors.New("no more sends")
}

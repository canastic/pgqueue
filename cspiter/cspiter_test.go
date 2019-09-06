package cspiter_test

import (
	"testing"
)

func TestEmptyBeforePullStarts(t *testing.T) {
	var foo Foo
	sends, receives := NewFooIterator(&foo)

	sends.Done()

	for receives.Pull() {
		t.Error("unexpected pull for closed iterator")
	}
	receives.Done()
}

func TestEmptyAfterPullStarts(t *testing.T) {
	var foo Foo
	sends, receives := NewFooIterator(&foo)

	readyChan := make(chan (<-chan bool))
	go func() {
		readyChan <- receives.StartPull()
	}()

	sends.Done()

	ready := <-readyChan

	select {
	case ready := <-ready:
		if ready {
			t.Error("unexpected pull for closed iterator")
		}
	}

	receives.Done()
}

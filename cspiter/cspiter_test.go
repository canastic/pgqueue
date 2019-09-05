package cspiter_test

import (
	"testing"
)

func TestEmpty(t *testing.T) {
	var foo Foo
	sends, receives := NewFooIterator(&foo)

	sends.Done()

	for receives.Pull() {
		t.Error("unexpected pull for closed iterator")
	}
	receives.Done()
}

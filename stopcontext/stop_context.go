// Package stopcontext extentds the standard Context interface with an
// additional Stopped signal, intended for gracefully stopping some iterative
// process associated with the Context.
package stopcontext

import (
	"context"
)

// Context wraps a context.Context. In addition to inheriting the wrapped
// context's Done signal, it has an additional Stopped signal that signals that
// some iterative process associated with the Context should stop on the next
// iteration, in a graceful manner.
//
// Context cancellation also triggers the Stopped signal, so you don't need to
// attend to both.
type Context interface {
	context.Context

	// Stopped works similarly to Done, except it's also triggered by a call to
	// a StopFunc returned by WithStop.
	Stopped() <-chan struct{}

	// isGracefulContext prevents other packages from implementing this; we
	// depend on Context always being a stopContext so that WithStop
	// knows how to chain them.
	isGracefulContext()
}

// Stopped is a helper function that calls Stopped if the given context has an
// ancestor created with WithStop from this package and Done otherwise.
//
// It is useful for iterative processes that want to support both the standard
// Context and the graceful stop provided by this package's Context.
func Stopped(ctx context.Context) <-chan struct{} {
	if ctx, ok := ctx.(Context); ok {
		return ctx.Stopped()
	}
	if ctx, ok := ctx.Value(stopCtxKey).(context.Context); ok {
		return ctx.Done()
	}
	return ctx.Done()
}

// A StopFunc is like a context.CancelFunc, except it triggers a Stopped signal.
type StopFunc = func()

// WithStop derives a Context whose Stopped signal is triggered by calling the
// returned StopFunc.
//
// If the parent context has an ancestor created with WithStop, the new
// Context's Stopped signal will propagate the parent's Stopped signal too.
func WithStop(parent context.Context) (Context, StopFunc) {
	stopParent := parent
	if p, ok := parent.Value(stopCtxKey).(context.Context); ok {
		// Hook the new stop context with the old one, whose cancellation
		// happens when its associated StopFunc is called, so that it triggers
		// a stop stop for the new one too.
		stopParent = p
	}
	ctx, cancel := context.WithCancel(stopParent)
	return stopContext{
		Context: context.WithValue(parent, stopCtxKey, ctx),
		ctx:     ctx,
	}, cancel
}

type stopContext struct {
	context.Context // parent
	ctx             context.Context
}

func (stopContext) isGracefulContext() {}

func (ctx stopContext) Stopped() <-chan struct{} {
	return ctx.ctx.Done()
}

var stopCtxKey = func() interface{} {
	type t struct{}
	return t{}
}()

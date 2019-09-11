package itersess

import (
	"context"
	"fmt"
)

type PullFunc func() bool

func (p *IdlePuller) Loop(ctx context.Context) (PullFunc, DoneFunc) {
	idle := true

	pull := func() bool {
		select {
		case <-ctx.Done():
			<-p.Finish
			idle = false
			return false
		default:
		}

		accepting := <-p.StartPull

		var waitingGive WaitingGivePuller
		select {
		case waitingGive = <-accepting.Accepted:
		case <-accepting.GiverDone:
			idle = false
			return false
		}

		select {
		case *p = <-waitingGive.Got:
			return true
		case <-waitingGive.GiverDone:
			idle = false
			return false
		case <-ctx.Done():
			<-waitingGive.Cancel
			idle = false
			return false
		}
	}

	done := func() {
		if !idle {
			return
		}
		<-p.Finish
	}

	return pull, done
}

type WaitPull func() bool

type DoneFunc func()

func (g *IdleGiver) Loop(ctx context.Context) (context.Context, WaitPull, DoneFunc) {
	ctx, cancel := context.WithCancel(ctx)
	loop := giverLoop{
		ctx:                      ctx,
		cancel:                   cancel,
		state:                    giverLoopStateIdle,
		g:                        g,
		pullerDoneWhilePreparing: make(chan struct{}),
		giveReady:                make(chan struct{}, 1),
	}
	return ctx, loop.waitPull, loop.done
}

type giverLoopState int

const (
	giverLoopStateIdle giverLoopState = iota
	giverLoopStatePreparingGive
	giverLoopStateFinished
)

type giverLoop struct {
	ctx    context.Context
	cancel context.CancelFunc

	state giverLoopState

	// only usable on giverLoopStateIdle
	g *IdleGiver

	// only usable on giverLoopStatePreparingGive
	preparing                PreparingGiveGiver
	pullerDoneWhilePreparing chan struct{}
	giveReady                chan struct{}
}

func (l *giverLoop) waitPull() bool {
	switch l.state {
	case giverLoopStateIdle:
		return l.waitPullIdle()
	case giverLoopStatePreparingGive:
		return l.waitPullPreparingGive()
	case giverLoopStateFinished:
		return false
	default:
		panic(fmt.Errorf("unexpected state: %v", l.state))
	}
}

func (l *giverLoop) waitPullIdle() bool {
	var accepting AcceptingPullGiver
	select {
	case accepting = <-l.g.StartAccepting:
	case <-l.ctx.Done():
		<-l.g.Finish
		l.state = giverLoopStateFinished
		return false
	}

	select {
	case l.preparing = <-accepting.Accepted:
	case <-accepting.PullerDone:
		l.state = giverLoopStateFinished
		return false
	}

	l.state = giverLoopStatePreparingGive

	preparing := l.preparing
	goo(func() {
		select {
		case <-preparing.PullerDone:
			close(l.pullerDoneWhilePreparing)
			l.cancel()
		case <-l.giveReady:
		case <-l.ctx.Done():
		}
	})

	return true
}

func (l *giverLoop) waitPullPreparingGive() bool {
	l.giveReady <- struct{}{}

	select {
	case *l.g = <-l.preparing.Gave:
		l.state = giverLoopStateIdle
	case <-l.preparing.PullerDone:
		l.state = giverLoopStateFinished
	case <-l.pullerDoneWhilePreparing:
		l.state = giverLoopStateFinished
	}

	return l.waitPull()
}

func (l *giverLoop) done() {
	switch l.state {
	case giverLoopStateIdle:
		<-l.g.Finish
	case giverLoopStatePreparingGive:
		l.cancel()

		select {
		case <-l.preparing.Finish:
		case <-l.preparing.PullerDone:
		case <-l.pullerDoneWhilePreparing:
		}
	case giverLoopStateFinished:
	}

	l.state = giverLoopStateFinished
}

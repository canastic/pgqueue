package itersess

import (
	"fmt"
)

//go:generate stringer -type msg .

type msg int

const (
	msgPullerDone msg = iota
	msgGiverDone
	msgPull
	msgGave
)

func unexpected(state string, msg msg) {
	panic(fmt.Errorf("itersess: %s: unexpected message received: %v", state, msg))
}

var goo = func(f func()) { go f() }

func New() (IdleGiver, IdlePuller) {
	msgs := make(chan msg)
	giver, runGiver := idleGiver(msgs)
	puller, runPuller := idlePuller(msgs)
	goo(runGiver.run)
	goo(runPuller.run)
	return giver, puller
}

type runner func() runner

func (r runner) run() {
	for r != nil {
		r = r()
	}
}

type IdleGiver struct {
	StartAccepting <-chan AcceptingPullGiver
	Finish         <-chan DoneGiver
}

type AcceptingPullGiver struct {
	Accepted   <-chan PreparingGiveGiver
	PullerDone <-chan DoneGiver
}

type PreparingGiveGiver struct {
	Gave       <-chan IdleGiver
	Finish     <-chan DoneGiver
	PullerDone <-chan DoneGiver
}

type DoneGiver struct{}

type IdlePuller struct {
	StartPull <-chan WaitingAcceptPuller
	Finish    <-chan DonePuller
}

type WaitingAcceptPuller struct {
	Accepted  <-chan WaitingGivePuller
	GiverDone <-chan DonePuller
}

type WaitingGivePuller struct {
	Got       <-chan IdlePuller
	GiverDone <-chan DonePuller
	Cancel    <-chan DonePuller
}

type DonePuller struct{}

func idleGiver(msgs chan msg) (IdleGiver, runner) {
	startAccepting := make(chan AcceptingPullGiver)
	finish := make(chan DoneGiver)

	return IdleGiver{
			StartAccepting: startAccepting,
			Finish:         finish,
		}, func() runner {
			acceptingPullGiver, runAcceptingPullGiver := acceptingPullGiver(msgs)
			select {
			case startAccepting <- acceptingPullGiver:
				return runAcceptingPullGiver
			case finish <- DoneGiver{}:
				msgs <- msgGiverDone
			}
			return nil
		}
}

func acceptingPullGiver(msgs chan msg) (AcceptingPullGiver, runner) {
	accepted := make(chan PreparingGiveGiver)
	pullerDone := make(chan DoneGiver)

	return AcceptingPullGiver{
			Accepted:   accepted,
			PullerDone: pullerDone,
		}, func() runner {
			preparingGiveGiver, runPreparingGiveGiver := preparingGiveGiver(msgs)
			switch msg := <-msgs; msg {
			case msgPull:
				accepted <- preparingGiveGiver
				return runPreparingGiveGiver
			case msgPullerDone:
				pullerDone <- DoneGiver{}
			default:
				unexpected("AcceptingPullGiver", msg)
			}
			return nil
		}
}

func preparingGiveGiver(msgs chan msg) (PreparingGiveGiver, runner) {
	gave := make(chan IdleGiver)
	finish := make(chan DoneGiver)
	pullerDone := make(chan DoneGiver)

	return PreparingGiveGiver{
			Gave:       gave,
			Finish:     finish,
			PullerDone: pullerDone,
		}, func() runner {
			idleGiver, runIdleGiver := idleGiver(msgs)
			select {
			case gave <- idleGiver:
				msgs <- msgGave
				return runIdleGiver
			case finish <- DoneGiver{}:
				select {
				case msgs <- msgGiverDone:
				case msg := <-msgs:
					switch msg {
					case msgPullerDone:
					default:
						unexpected("PreparingGiveGiver", msg)
					}
				}
			case msg := <-msgs:
				switch msg {
				case msgPullerDone:
					pullerDone <- DoneGiver{}
				default:
					unexpected("PreparingGiveGiver", msg)
				}
			}
			return nil
		}
}

func idlePuller(msgs chan msg) (IdlePuller, runner) {
	startPull := make(chan WaitingAcceptPuller)
	finish := make(chan DonePuller)
	return IdlePuller{
			StartPull: startPull,
			Finish:    finish,
		}, func() runner {
			waitingAcceptPuller, runWaitingAcceptPuller := waitingAcceptPuller(msgs)
			select {
			case startPull <- waitingAcceptPuller:
				return runWaitingAcceptPuller
			case finish <- DonePuller{}:
				msgs <- msgPullerDone
			}
			return nil
		}
}

func waitingAcceptPuller(msgs chan msg) (WaitingAcceptPuller, runner) {
	accepted := make(chan WaitingGivePuller)
	giverDone := make(chan DonePuller)

	return WaitingAcceptPuller{
			Accepted:  accepted,
			GiverDone: giverDone,
		}, func() runner {
			select {
			case msgs <- msgPull:
				waitingGivePuller, runWaitingGivePuller := waitingGivePuller(msgs)
				accepted <- waitingGivePuller
				return runWaitingGivePuller
			case msg := <-msgs:
				switch msg {
				case msgGiverDone:
					giverDone <- DonePuller{}
				default:
					unexpected("WaitingAcceptPuller", msg)
				}
			}
			return nil
		}
}

func waitingGivePuller(msgs chan msg) (WaitingGivePuller, runner) {
	got := make(chan IdlePuller)
	giverDone := make(chan DonePuller)
	cancel := make(chan DonePuller)

	return WaitingGivePuller{
			Got:       got,
			GiverDone: giverDone,
			Cancel:    cancel,
		}, func() runner {
			select {
			case cancel <- DonePuller{}:
				select {
				case msgs <- msgPullerDone:
				case msg := <-msgs:
					switch msg {
					case msgGave:
						msgs <- msgPullerDone
					case msgGiverDone:
					default:
						unexpected("WaitingGivePuller", msg)
					}
				}
			case msg := <-msgs:
				switch msg {
				case msgGave:
					idlePuller, runIdlePuller := idlePuller(msgs)
					got <- idlePuller
					return runIdlePuller
				case msgGiverDone:
					giverDone <- DonePuller{}
				default:
					unexpected("WaitingGivePuller", msg)
				}
			}
			return nil
		}
}

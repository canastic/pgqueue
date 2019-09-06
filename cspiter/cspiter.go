package cspiter

func NewIterator() (Sends, Receives) {
	pulls := make(chan struct{})
	ready := make(chan bool)
	closed := make(chan struct{})
	return Sends{
			Pulls:  pulls,
			Ready:  ready,
			Closed: closed,
		}, Receives{
			Pulls:  pulls,
			Ready:  ready,
			Closed: closed,
		}
}

type Sends struct {
	Pulls  <-chan struct{}
	Ready  chan<- bool
	Closed chan<- struct{}
}

func (s Sends) Done() {
	close(s.Closed)
	close(s.Ready)
}

type Receives struct {
	Pulls  chan<- struct{}
	Ready  <-chan bool
	Closed <-chan struct{}
}

func (r Receives) StartPull() (ready <-chan bool) {
	select {
	case <-r.Closed:
	case r.Pulls <- struct{}{}:
	}
	return r.Ready
}

func (r Receives) Pull() bool {
	return <-r.StartPull()
}

func (r Receives) Done() {
	close(r.Pulls)
}

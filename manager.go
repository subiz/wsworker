package wsworker

import (
	"net/http"
	"time"
)

type Mgr struct {
	newConnC    chan *Conn
	newConnErrC chan error
	msgChan     chan *Message
	stopped     bool
	hasC        chan string
	hasReplyC   chan bool
}

type Conn struct {
	Id    string
	R     *http.Request
	W     http.ResponseWriter
	Intro []byte
}

var (
	PingDeadline    = 3 * time.Minute
	OutdateDeadline = 2 * time.Minute
	DeadDeadline    = 2 * time.Minute
)

func runManager(m *Mgr, deadChan chan<- string, commitChan chan<- Commit) {
	workers := make(map[string]*Worker, 1000000)
	pingTicker := time.NewTicker(PingDeadline)
	outdateTicker := time.NewTicker(OutdateDeadline)
	deadTicker := time.NewTicker(DeadDeadline)
	commitTicker := time.NewTicker(1 * time.Second)
	defer func() {
		m.clearRun([]*time.Ticker{commitTicker, deadTicker, outdateTicker, pingTicker})
		for _, w := range workers {
			w.Close()
		}
	}()
	m.stopped = false
	for !m.stopped {
		select {
		case <-pingTicker.C: // ping all after PingDeadline
			for _, w := range workers {
				w.PingCheck(PingDeadline)
			}
		case <-deadTicker.C: // move all closed ws to dead state
			for _, w := range workers {
				if w.state == DEAD { // clean workers
					delete(workers, w.id)
				}
				w.DeadCheck(DeadDeadline)
			}
		case <-commitTicker.C: // commit all dirty ws
			for _, w := range workers {
				w.CommitCheck()
			}
		case <-outdateTicker.C: // remove all outdated ws
			for _, w := range workers {
				w.OutdateCheck(OutdateDeadline)
			}
		case conn := <-m.newConnC:
			if w := workers[conn.Id]; w != nil {
				m.newConnErrC <- w.SetConnection(conn.R, conn.W, conn.Intro)
				break
			}
			w := NewWorker(conn.Id, deadChan, commitChan)
			err := w.SetConnection(conn.R, conn.W, conn.Intro)
			if err == nil {
				workers[conn.Id] = w
			}
			m.newConnErrC <- err
		case msg := <-m.msgChan:
			w := workers[msg.Id]
			if w == nil {
				w = NewWorker(msg.Id, deadChan, commitChan)
				workers[msg.Id] = w
			}
			w.Send(msg)
		case id := <-m.hasC:
			_, ok := workers[id]
			m.hasReplyC <- ok
		}
	}
}

func NewManager(deadChan chan<- string, commitChan chan<- Commit) *Mgr {
	m := &Mgr{
		newConnC:    make(chan *Conn),
		newConnErrC: make(chan error),
		msgChan:     make(chan *Message),
		hasC:        make(chan string),
		hasReplyC:   make(chan bool),
	}
	go runManager(m, deadChan, commitChan)
	return m
}

func (m *Mgr) SetConnection(r *http.Request, w http.ResponseWriter, id string, intro []byte) error {
	defer func() { recover() }()
	m.newConnC <- &Conn{Id: id, R: r, W: w, Intro: intro}
	return <-m.newConnErrC
}

func (m *Mgr) Send(id string, offset int64, payload []byte) {
	defer func() { recover() }()
	m.msgChan <- &Message{Id: id, Offset: offset, Payload: payload}
}

func (m *Mgr) Stop() {
	m.stopped = true
}

func (m *Mgr) Has(id string) bool {
	defer func() { recover() }()
	m.hasC <- id
	return <-m.hasReplyC
}

func (m *Mgr) clearRun(tickers []*time.Ticker) {
	for _, t := range tickers {
		select {
		case <-t.C:
		default:
		}
		t.Stop()
	}

	close(m.newConnC)
	close(m.newConnErrC)
	close(m.msgChan)
	close(m.hasC)
	close(m.hasReplyC)
}

package wsworker

import (
	"net/http"
	"time"
)

type Mgr struct {
	newConnC    chan *Conn
	newConnErrC chan error
}

type Conn struct {
	Id string
	R  *http.Request
	W  http.ResponseWriter
}

var PingDeadline = 3 * time.Minute
var OutdateDeadline = 2 * time.Minute
var DeadDeadline = 2 * time.Minute

func runManager(m *Mgr, msgChan <-chan *Message, deadChan chan<- string, commitChan chan<- Commit) {
	pingTicker := time.NewTicker(PingDeadline)
	outdateTicker := time.NewTicker(OutdateDeadline)
	deadTicker := time.NewTicker(DeadDeadline)
	commitTicker := time.NewTicker(1 * time.Second)
	workers := make(map[string]IWorker, 1000000)
	for {
		select {
		case <-pingTicker.C: // ping all after PingDeadline
			for _, w := range workers {
				w.PingCheck(PingDeadline)
			}
		case <-deadTicker.C: // move all closed ws to dead state
			for _, w := range workers {
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
			w := workers[conn.Id]
			if w == nil {
				w = NewWorker(conn.Id, deadChan, commitChan)
				workers[conn.Id] = w
			}

			m.newConnErrC <- w.SetConnection(conn.R, conn.W)
		case msg := <-msgChan:
			if w := workers[msg.Id]; w != nil {
				w.Send(msg)
			}
		}
	}
}

func NewManager(msgChan <-chan *Message, deadChan chan<- string, commitChan chan<- Commit) *Mgr {
	m := &Mgr{
		newConnC:    make(chan *Conn),
		newConnErrC: make(chan error),
	}
	go runManager(m, msgChan, deadChan, commitChan)
	return m
}

func (m *Mgr) SetConnection(r *http.Request, w http.ResponseWriter, id string) error {
	m.newConnC <- &Conn{Id: id, R: r, W: w}
	return <-m.newConnErrC
}

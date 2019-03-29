package wsworker

import (
	"net/http"
	"sync"
	"time"
)

type Mgr struct {
	*sync.RWMutex
	stopped    bool
	workers    map[string]*Worker
	deadChan   chan<- string
	commitChan chan<- Commit
}

var (
	PingDeadline    = 15 * time.Second
	OutdateDeadline = 2 * time.Minute
	DeadDeadline    = 2 * time.Minute
)

func (m *Mgr) checkPing() {
	for !m.stopped {
		m.RLock()
		for _, w := range m.workers {
			m.RUnlock()
			w.PingCheck()
			m.RLock()
		}
		m.RUnlock()
		time.Sleep(PingDeadline)
	}
}

func (m *Mgr) cleanDeadWorkers() {
	for !m.stopped {
		m.RLock()
		for _, w := range m.workers {
			m.RUnlock()
			if w.GetState() == DEAD {
				m.Lock()
				delete(m.workers, w.id)
				m.Unlock()
			}
			m.RLock()
		}
		m.RUnlock()
		time.Sleep(DeadDeadline)
	}
}

func (m *Mgr) checkDead() {
	for !m.stopped {
		m.RLock()
		for _, w := range m.workers {
			m.RUnlock()
			w.DeadCheck(DeadDeadline)
			m.RLock()
		}
		m.RUnlock()
		time.Sleep(DeadDeadline)
	}
}

func (m *Mgr) checkOutdate() {
	for !m.stopped {
		m.RLock()
		for _, w := range m.workers {
			m.RUnlock()
			w.OutdateCheck(OutdateDeadline)
			m.RLock()
		}
		m.RUnlock()
		time.Sleep(OutdateDeadline)
	}
}

func (m *Mgr) doCommit() {
	for !m.stopped {
		m.RLock()
		for _, w := range m.workers {
			m.RUnlock()
			w.CommitCheck()
			m.RLock()
		}
		m.RUnlock()
		time.Sleep(1 * time.Second)
	}
}

func NewManager(deadChan chan<- string, commitChan chan<- Commit) *Mgr {
	m := &Mgr{
		RWMutex:    &sync.RWMutex{},
		deadChan:   deadChan,
		commitChan: commitChan,
		workers:    make(map[string]*Worker, 1000),
		stopped:    false,
	}
	go m.doCommit()
	go m.checkOutdate()
	go m.checkDead()
	go m.cleanDeadWorkers()
	go m.checkPing()
	return m
}

func (m *Mgr) SetConnection(r *http.Request, w http.ResponseWriter, id string, intro []byte) error {
	m.RLock()
	worker := m.workers[id]
	m.RUnlock()
	if worker == nil {
		worker = NewWorker(id, m.deadChan, m.commitChan)
		m.Lock()
		m.workers[id] = worker
		m.Unlock()
	}
	return worker.SetConnection(r, w, intro)
}

func (m *Mgr) Send(id string, offset int64, payload []byte) {
	m.RLock()
	w := m.workers[id]
	m.RUnlock()

	if w == nil {
		w = NewWorker(id, m.deadChan, m.commitChan)
		m.Lock()
		m.workers[id] = w
		m.Unlock()
	}

	w.Send(&message{Offset: offset, Payload: payload})
}

func (m *Mgr) Stop() {
	m.Lock()
	m.stopped = true
	for _, w := range m.workers {
		//		m.Unlock()
		w.Halt()
		//	m.Lock()
	}
	m.Unlock()
}

func (m *Mgr) Has(id string) bool {
	m.RLock()
	_, ok := m.workers[id]
	m.RUnlock()
	return ok
}

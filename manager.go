package wsworker

import (
	"net/http"
	"time"
)

// Mgr manages websocket connections
// each connection is managed by a worker
type Mgr struct {
	workers Map

	stopped    bool
	deadChan   chan<- string
	commitChan chan<- Commit
}

// NewManager creates a new Mgr object
func NewManager(deadChan chan<- string, commitChan chan<- Commit) *Mgr {
	m := &Mgr{
		workers:    NewMap(),
		deadChan:   deadChan,
		commitChan: commitChan,
		stopped:    false,
	}
	go m.doCommit()
	go m.checkOutdate()
	go m.checkDead()
	go m.cleanDeadWorkers()
	go m.checkPing()
	return m
}

// checkPing runs ping check loop
func (me *Mgr) checkPing() {
	for !me.stopped {
		me.workers.Scan(func(_ string, w interface{}) { w.(*Worker).PingCheck() })
		time.Sleep(PingDeadline)
	}
}

// cleanDeadWorker runs a loop which removes dead workers every DeadDeadline seconds
func (me *Mgr) cleanDeadWorkers() {
	for !me.stopped {
		me.workers.Scan(func(id string, w interface{}) {
			if w.(*Worker).GetState() == DEAD {
				me.workers.Delete(id)
			}
		})
		time.Sleep(DeadDeadline)
	}
}

// checkDead runs a loop which call DeadCheck on every worker
func (me *Mgr) checkDead() {
	for !me.stopped {
		me.workers.Scan(func(_ string, w interface{}) { w.(*Worker).DeadCheck(DeadDeadline) })
		time.Sleep(DeadDeadline)
	}
}

// checkOutdate runs a loop which call OutdateCheck on every worker
func (me *Mgr) checkOutdate() {
	for !me.stopped {
		me.workers.Scan(func(_ string, w interface{}) { w.(*Worker).OutdateCheck(OutdateDeadline) })
		time.Sleep(OutdateDeadline)
	}
}

// doCommit runs a loop which call CommitCheck on every worker
func (me *Mgr) doCommit() {
	for !me.stopped {
		me.workers.Scan(func(_ string, w interface{}) { w.(*Worker).CommitCheck() })
		time.Sleep(1 * time.Second)
	}
}

func (m *Mgr) SetConnection(r *http.Request, w http.ResponseWriter, id string, intro []byte) error {
	wi, ok := m.workers.Get(id)
	var worker *Worker
	if ok {
		worker = wi.(*Worker)
	} else {
		worker = NewWorker(id, m.deadChan, m.commitChan)
		m.workers.Set(id, worker)
	}
	return worker.SetConnection(r, w, intro)
}

func (m *Mgr) Send(id string, offset int64, payload []byte) {
	wi, _ := m.workers.Get(id)
	worker := wi.(*Worker)

	if worker == nil {
		worker = NewWorker(id, m.deadChan, m.commitChan)
		m.workers.Set(id, worker)
	}
	worker.Send(&message{Offset: offset, Payload: payload})
}

func (me *Mgr) Stop() {
	me.stopped = true
	me.workers.Scan(func(_ string, w interface{}) { w.(*Worker).Halt() })
}

func (m *Mgr) Has(id string) bool {
	_, ok := m.workers.Get(id)
	return ok
}

var (
	PingDeadline    = 15 * time.Second
	OutdateDeadline = 2 * time.Minute
	DeadDeadline    = 2 * time.Minute
)

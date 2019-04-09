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
	commitChan chan<- int64
}

// NewManager creates a new Mgr object
func NewManager(deadChan chan<- string, commitChan chan<- int64) *Mgr {
	m := &Mgr{workers: NewMap(), deadChan: deadChan, commitChan: commitChan}
	go m.doCommit()
	go m.checkOutdate()
	go m.checkDead()
	go m.cleanDeadWorkers()
	go m.doPing()
	return m
}

// checkPing runs ping check loop
func (me *Mgr) doPing() {
	for !me.stopped {
		me.workers.Scan(func(_ string, w interface{}) { w.(*Worker).Ping() })
		time.Sleep(PingInterval)
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
		me.workers.Scan(func(_ string, w interface{}) { w.(*Worker).DeadCheck() })
		time.Sleep(DeadDeadline)
	}
}

// checkOutdate runs a loop which call OutdateCheck on every worker
func (me *Mgr) checkOutdate() {
	for !me.stopped {
		me.workers.Scan(func(_ string, w interface{}) { w.(*Worker).OutdateCheck() })
		time.Sleep(OutdateDeadline)
	}
}

// doCommit runs a loop which call CommitCheck on every worker
func (me *Mgr) doCommit() {
	for !me.stopped {
		me.workers.Scan(func(_ string, w interface{}) {
			for _, offset := range w.(*Worker).Commit() {
				me.commitChan <- offset // TODO: must go through offset mgr
			}
		})
		time.Sleep(5 * time.Second)
	}
}

// makeSureWorker returns existings worker or creates a new worker if id not found
func (me *Mgr) makeSureWorker(id string) *Worker {
	wi, ok := me.workers.Get(id)
	if !ok {
		w := NewWorker(id, me.deadChan)
		me.workers.Set(id, w)
		return w
	}
	return wi.(*Worker)
}

func (me *Mgr) Connect(r *http.Request, w http.ResponseWriter, id string, intro []byte) error {
	return me.makeSureWorker(id).SetConnection(r, w, intro)
}

func (me *Mgr) Send(id string, offset int64, payload []byte) {
	me.makeSureWorker(id).Send(offset, payload)
}

func (me *Mgr) Stop() {
	me.stopped = true
	me.workers.Scan(func(_ string, w interface{}) { w.(*Worker).Halt() })
}

var (
	PingInterval    = 15 * time.Second
	OutdateDeadline = 2 * time.Minute
	DeadDeadline    = 2 * time.Minute
)

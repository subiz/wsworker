package wsworker

import (
	"net/http"
	"time"
)

// Mgr manages websocket connections
// each connection is managed by a worker
type Mgr struct {
	workers Map

	stopped bool

	// a channel of worker's ID, used notify the manager that the worker is dead
	deadChan   chan<- string
	commitChan chan<- int64
}

// NewManager creates a new Mgr object
func NewManager(deadChan chan<- string, commitChan chan<- int64) *Mgr {
	m := &Mgr{workers: NewMap(), deadChan: deadChan, commitChan: commitChan}
	go m.doCommit()
	go m.cleanDeadWorkers()
	go m.doPing()
	return m
}

// cleanDeadWorker runs a loop which removes dead workers
func (me *Mgr) cleanDeadWorkers() {
	// holds dead workers
	deadWorkers := make(map[string]int64)

	for !me.stopped {
		now := time.Now().Unix()
		me.workers.Scan(func(id string, w interface{}) {
			exp_sec := deadWorkers[id]
			if exp_sec == 0 {
				if w.(*Worker).GetState() == DEAD {
					deadWorkers[id] = now + 3600 //
					me.deadChan <- id
				}
				return
			}

			if exp_sec < now { // clear the dead worker after 1 hour
				me.workers.Delete(id)
				delete(deadWorkers, id)
			}
		})
		time.Sleep(30 * time.Minute)
	}
}

// checkPing runs ping check loop
func (me *Mgr) doPing() {
	for !me.stopped {
		me.workers.Scan(func(_ string, w interface{}) { w.(*Worker).Ping() })
		time.Sleep(PingInterval)
	}
}

// doCommit runs a loop which call CommitCheck on every worker
func (me *Mgr) doCommit() {
	for !me.stopped {
		me.workers.Scan(func(id string, w interface{}) {
			for _, offset := range w.(*Worker).Commit() {
				me.commitChan <- offset
			}
		})
		time.Sleep(5 * time.Second)
	}
}

func (me *Mgr) Connect(r *http.Request, w http.ResponseWriter, id string, intro []byte) error {
	worker := me.workers.GetOrCreate(id, func() interface{} { return NewWorker(id) }).(*Worker)
	return worker.Connect(r, w, intro)
}

func (me *Mgr) Send(id string, offset int64, payload []byte) {
	worker := me.workers.GetOrCreate(id, func() interface{} { return NewWorker(id) }).(*Worker)
	if err := worker.Send(offset, payload); err == DEADERR {
		// the worker is dead and unable to handle the message
		// we ignores the message by committing it
		me.commitChan <- offset
	}
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

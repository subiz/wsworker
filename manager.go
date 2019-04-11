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

	deadWorkers *Cache
	// a channel of worker's ID, used notify the manager that the worker is dead
	deadChan   chan<- string
	commitChan chan<- int64
}

// NewManager creates a new Mgr object
func NewManager(redisHosts []string, redisPw string, deadChan chan<- string, commitChan chan<- int64) *Mgr {
	m := &Mgr{workers: NewMap(), deadChan: deadChan, commitChan: commitChan}
	deadWorkers, err := NewCache(redisHosts, redisPw, "deadWs24", 100000)
	if err != nil {
		panic(err)
	}
	m.deadWorkers = deadWorkers
	go m.doCommit()
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

// doCommit runs a loop which call CommitCheck on every worker
func (me *Mgr) doCommit() {
	for !me.stopped {
		me.workers.Scan(func(id string, w interface{}) {
			offsets, err := w.(*Worker).Commit()
			if err == DEADERR {
				me.deadChan <- id
				me.deadWorkers.Set(id, []byte("OK"))
				return
			}
			for _, offset := range offsets {
				me.commitChan <- offset
			}
		})
		time.Sleep(5 * time.Second)
	}
}

func (me *Mgr) Connect(r *http.Request, w http.ResponseWriter, id string, intro []byte) error {
	_, has, err := me.deadWorkers.Get(id)
	if err != nil {
		return err
	}

	if has {
		return DEADERR
	}

	worker := me.workers.GetOrCreate(id, func() interface{} { return NewWorker(id) }).(*Worker)
	return worker.Attach(r, w, intro)
}

func (me *Mgr) Send(id string, offset int64, payload []byte) error {
	_, has, err := me.deadWorkers.Get(id)
	if err != nil {
		return err
	}

	if has {
		me.commitChan <- offset
		return DEADERR
	}

	worker := me.workers.GetOrCreate(id, func() interface{} { return NewWorker(id) }).(*Worker)
	if err := worker.Send(offset, payload); err == DEADERR {
		// the worker is dead and unable to handle the message
		// we ignores the message by committing it
		me.commitChan <- offset
	}
	return nil
}

func (me *Mgr) Stop() {
	me.stopped = true
	me.workers.Scan(func(_ string, w interface{}) { w.(*Worker).Stop() })
}

var (
	PingInterval    = 15 * time.Second
)

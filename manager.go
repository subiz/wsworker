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
	go m.doPing()
	return m
}

// checkPing runs ping check loop
func (me *Mgr) doPing() {
	for !me.stopped {
		me.workers.Scan(func(id string, w interface{}) {
			err := w.(*Worker).Ping()
			if err != DEADERR {
				return
			}
			// dead
			me.deadChan <- id
			me.deadWorkers.Set(id, []byte("OK"))
			me.workers.Delete(id)
		})
		time.Sleep(15 * time.Second)
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

	worker := me.workers.GetOrCreate(id, func() interface{} { return NewWorker(id, me.commitChan) }).(*Worker)
	return worker.Attach(r, w, intro)
}

func (me *Mgr) Send(id string, offset int64, payload []byte) error {
	if id == "" {
		me.commitChan <- offset
		return EMPTYCONNECTIONERR
	}
	_, has, err := me.deadWorkers.Get(id)
	if err != nil {
		me.commitChan <- offset
		return err
	}

	if has {
		me.commitChan <- offset
		return DEADERR
	}

	worker := me.workers.GetOrCreate(id, func() interface{} { return NewWorker(id, me.commitChan) }).(*Worker)
	worker.Send(offset, payload)
	return nil
}

func (me *Mgr) Stop() {
	me.stopped = true
	me.workers.Scan(func(_ string, w interface{}) { w.(*Worker).Stop() })
}

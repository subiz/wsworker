package wsworker

import (
	"errors"
	"github.com/subiz/wsworker/driver/gorilla"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

var (
	DEAD    = "dead"
	NORMAL  = "normal"
	DEADERR = errors.New("dead")
)

type Ws interface {
	Close() error
	Recv() ([]byte, error)
	Ping() error
	Send(data []byte) error // do not panic
}

type message struct {
	offset  int64
	payload []byte
	sent    int64 // unix second tells when message is sent
}

type Worker struct {
	*sync.Mutex

	// an unique string used by the manager to identify workers
	Id string

	// holds offset of the latest commit received from the client
	latest_committed_offset int64

	detached int64 // last detached time in second

	ws Ws // used to communicate with the websocket connection

	// holds the current mode of the worker, could be NORMAL, CLOSED, DEAD, ...
	state string

	// number of second that worker must commit when receive send
	// or it will be killed
	commitDeadline int64

	closeDeadline int64

	buffer []message
}

// NewWorker creates a new Worker object
func NewWorker(id string) *Worker {
	return &Worker{
		Mutex:          &sync.Mutex{},
		Id:             id,
		state:          NORMAL,
		detached:       time.Now().Unix(),
		closeDeadline:  int64(DeadDeadline.Seconds()),
		commitDeadline: int64(OutdateDeadline.Seconds()),
		buffer:         make([]message, 0, 32),
	}
}

// Halt forces worker to switch to DEAD mode
func (w *Worker) Halt() {
	w.Lock()
	defer w.Unlock()
	w.toDead()
}

func (me *Worker) Connect(r *http.Request, w http.ResponseWriter, intro []byte) error {
	me.Lock()
	defer me.Unlock()

	if me.state == DEAD {
		return DEADERR
	}

	ws, err := gorilla.NewWs(w, r, nil)
	if err != nil {
		return err
	}

	me.detachConnection() // detach old connection if exists
	me.ws = ws
	if intro != nil {
		if err := me.ws.Send(intro); err != nil {
			me.detachConnection()
			return err
		}
	}

	// replay old messages
	for _, msg := range me.buffer {
		if err := me.ws.Send(msg.payload); err != nil {
			log.Printf("[wsworker: %s] on send error %v", me.Id, err)
			me.detachConnection()
			return err
		}
	}

	go func() {
		me.Lock()
		// locked region
		defer me.Unlock()
		ws := me.ws
		for me.state == NORMAL && ws != nil {
			me.Unlock() // end locked region
			p, err := ws.Recv()
			me.Lock()        // start locked region
			if me.ws != ws { // worker has detached this connection
				return
			}
			if err != nil {
				log.Printf("[wsworker: %s] to error, normal recv %v", me.Id, err)
				me.detachConnection()
				return
			}
			offset, _ := strconv.ParseInt(string(p), 10, 0)
			if offset < me.latest_committed_offset { // ignore invalid offset
				continue
			}
			me.latest_committed_offset = offset
		}
	}()
	return nil
}

// toDead switchs worker to DEAD mode, release all holding resources
// once worker is dead, it cannot be reused. User should not Send message on dead ws
// Note: this function is not thread safe, caller should lock the worker before use
func (me *Worker) toDead() {
	if me.state == DEAD {
		return
	}
	log.Printf("[wsworker: %s] onDead", me.Id)

	me.state = DEAD
	me.detachConnection()
	if len(me.buffer) != 0 { // release all dirty messages
		lastmsg := me.buffer[len(me.buffer)-1]
		me.latest_committed_offset = lastmsg.offset
	}
}

// detachConnection closes the current websocket connection and remove it
// out of worker
// Note: this function is not thread safe, caller should lock the worker before use
func (me *Worker) detachConnection() {
	if me.ws == nil {
		return
	}
	me.ws.Close()
	me.detached = time.Now().Unix()
	me.ws = nil
}

// Ping delivers a ping message to the client
// returns the current state of the worker
func (me *Worker) Ping() {
	me.Lock()
	defer me.Unlock()

	switch me.state {
	case DEAD:
		return
	case NORMAL:
		if me.ws == nil { // worker is detached from the connection
			now := time.Now().Unix() // second
			if now-me.detached < me.closeDeadline {
				return
			}
			log.Printf("[wsworker: %s] dead by close check", me.Id)
			me.toDead()
			return
		}
		if err := me.ws.Ping(); err != nil {
			log.Printf("[wsworker: %s] ping err %v", me.Id, err)
			me.detachConnection()
			return
		}
	}
}

// Commit returns commited offsets
func (me *Worker) Commit() []int64 {
	me.Lock()
	defer me.Unlock()

	var committed_offsets []int64
	for i := range me.buffer {
		if me.buffer[i].offset <= me.latest_committed_offset {
			committed_offsets = append(committed_offsets, me.buffer[i].offset)
			continue
		}

		if i > 0 { // me.buffer[0:] is slow (maybe)
			me.buffer = me.buffer[i:]
		}

		if len(me.buffer) > 0 {
			// kills worker if client don't commit in time
			firstmsg := me.buffer[0]
			timesincefirstsent := time.Now().Unix() - firstmsg.sent // second
			if timesincefirstsent > me.commitDeadline {
				log.Printf("[wsworker: %s] dead by late commit", me.Id)
				me.toDead()
			}
		}
		return committed_offsets
	}
	me.buffer = nil
	return committed_offsets
}

// Send delivers a message to the client
// ignore the request if the worker is DEAD
func (me *Worker) Send(offset int64, payload []byte) error {
	me.Lock()
	defer me.Unlock()

	if me.state == DEAD {
		return DEADERR
	}
	now := time.Now().Unix()
	me.buffer = append(me.buffer, message{offset: offset, payload: payload, sent: now})
	if len(me.buffer) > 20000 {
		log.Printf("[wsworker: %s] dead by buffer overflow", me.Id)
		me.toDead()
		return DEADERR
	}

	if me.state == NORMAL && me.ws != nil {
		if err := me.ws.Send(payload); err != nil {
			log.Printf("[wsworker: %s] on send error %v", me.Id, err)
			me.detachConnection()
			return nil
		}
	}
	return nil
}

// GetState returns current state of the worker, could be "dead", "normal" or "closed"
func (me *Worker) GetState() string {
	me.Lock()
	defer me.Unlock()
	return me.state
}

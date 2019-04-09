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
	CLOSED  = "closed"
	DEAD    = "dead"
	NORMAL  = "normal"
	DEADERR = errors.New("dead")
)

type Ws interface {
	IsClosed() bool
	Close() error
	Recv() ([]byte, error)
	Ping() error
	Send(data []byte) error // do not panic
}

type message struct {
	Offset  int64
	Payload []byte
}

type Worker struct {
	*sync.Mutex

	// an unique string used by the manager to identify workers
	Id string

	// holds offset of the latest commit received from the client
	latest_committed_offset int64

	closed int64 // last closed time

	ws Ws // used to communicate with the websocket connection

	// holds the current mode of the worker, could be NORMAL, CLOSED, DEAD, ...
	state string

	// a channel of worker's ID, used notify the manager that the worker is dead
	deadChan chan<- string

	// number of second that worker must commit when receive send
	// or it will be killed
	commitDeadline int64

	closeDeadline int64

	first_dirty_sent_sec int64 // unix sec record the time of the first message sent after the last commited

	buffer []message
}

// NewWorker creates a new Worker object
func NewWorker(id string, deadChan chan<- string) *Worker {
	return &Worker{
		Mutex:          &sync.Mutex{},
		Id:             id,
		state:          CLOSED,
		closed:         time.Now().Unix(),
		deadChan:       deadChan,
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

func (me *Worker) SetConnection(r *http.Request, w http.ResponseWriter, intro []byte) error {
	me.Lock()
	defer me.Unlock()

	ws, err := gorilla.NewWs(w, r, nil)
	if err != nil {
		return err
	}

	switch me.state {
	case CLOSED, NORMAL:
		me.toClosed() // close the last websocket connection if existed

		me.ws = ws
		if intro != nil {
			if err := me.ws.Send(intro); err != nil {
				return err
			}
		}

		for _, m := range me.buffer {
			if err := me.ws.Send(m.Payload); err != nil {
				log.Printf("[wsworker: %s] on send error %v", me.Id, err)
				me.toClosed()
				return nil
			}
		}
		me.toNormal()

	case DEAD:
		return DEADERR
	}
	return nil
}

// toNormal switchs worker to NORMAL state
// Note: this function is not thread safe, caller should lock the worker before use
func (me *Worker) toNormal() {
	if me.state == NORMAL {
		return
	}
	me.state = NORMAL
	go func() {
		ws := me.ws
		me.Lock()
		for me.state == NORMAL && !ws.IsClosed() {
			me.Unlock()
			p, err := ws.Recv()
			me.Lock()

			if ws != me.ws {
				me.Unlock()
				return
			}

			if err != nil {
				log.Printf("[wsworker: %s] to error, normal recv %v", me.Id, err)
				me.toClosed()
				continue
			}

			offset, _ := strconv.ParseInt(string(p), 10, 0)
			if offset < me.latest_committed_offset { // ignore invalid offset
				continue
			}
			me.latest_committed_offset = offset
		}
	}()
}

// toClosed switchs worker to CLOSED mode
// Note: this function is not thread safe, caller should lock the worker before use
func (me *Worker) toClosed() {
	if me.state == CLOSED {
		return
	}
	me.state = CLOSED
	me.closed = time.Now().Unix()
	log.Printf("[wsworker: %s] CLOSING", me.Id)
	if me.ws != nil {
		me.ws.Close()
		me.ws = nil
	}
}

// DeadCheck kills worker which is closed for too long
func (me *Worker) DeadCheck() {
	me.Lock()
	defer me.Unlock()

	if me.state != CLOSED {
		return
	}

	now := time.Now().Unix() // second
	if now-me.closed < me.closeDeadline {
		return
	}
	log.Printf("[wsworker: %s] dead by close check %v", me.Id)
	me.toDead()
}

// toDead switchs worker to DEAD mode, release all holding resources
// once worker is dead, it cannot be reused, user should not call any method
// (execept GetState) afterward.
// Note: this function is not thread safe, caller should lock the worker before use
func (me *Worker) toDead() {
	if me.state == DEAD {
		return
	}
	log.Printf("[wsworker: %s] onDead", me.Id)

	me.state = DEAD
	if me.ws != nil {
		me.ws.Close()
		me.ws = nil
	}
	if len(me.buffer) != 0 { // release all dirty messages
		lastmsg := me.buffer[len(me.buffer)-1]
		me.latest_committed_offset = lastmsg.Offset
		me.buffer = nil
	}
	me.deadChan <- me.Id
}

// Ping delivers a ping message to the client
func (me *Worker) Ping() {
	me.Lock()
	defer me.Unlock()

	if me.state != NORMAL {
		return
	}

	if err := me.ws.Ping(); err != nil {
		log.Printf("[wsworker: %s] ping err %v", me.Id, err)
		me.toClosed()
		return
	}
}

// Commit returns commited offsets
func (me *Worker) Commit() []int64 {
	me.Lock()
	defer me.Unlock()

	if me.state != NORMAL {
		return nil
	}

	var committed_offsets []int64
	for i := range me.buffer {
		if me.latest_committed_offset < me.buffer[i].Offset {
			if i == 0 { // too small
				return nil
			}
			me.buffer = me.buffer[i:]
			return committed_offsets
		}
		committed_offsets = append(committed_offsets, me.buffer[i].Offset)
	}
	me.buffer = nil
	return committed_offsets
}

// OutdateCheck kills worker if client don't commit in time
// dealine is number of second
func (me *Worker) OutdateCheck() {
	me.Lock()
	defer me.Unlock()

	now := time.Now().Unix() // second
	if me.first_dirty_sent_sec == 0 ||
		now-me.first_dirty_sent_sec < me.commitDeadline {
		return
	}

	log.Printf("[wsworker: %s] dead by late commit", me.Id)
	me.toDead()
}

// Send delivers a message to the client
func (me *Worker) Send(offset int64, payload []byte) {
	me.Lock()
	defer me.Unlock()

	if me.state == DEAD {
		if offset > me.latest_committed_offset {
			me.latest_committed_offset = offset
		}
		return
	}

	// state == NORMAL and CLOSED
	me.buffer = append(me.buffer, message{Offset: offset, Payload: payload})
	if len(me.buffer) > 20000 {
		log.Printf("[wsworker: %s] dead by buffer overflow", me.Id)
		me.toDead()
		return
	}

	if me.state == NORMAL {
		if err := me.ws.Send(payload); err != nil {
			log.Printf("[wsworker: %s] on send error %v", me.Id, err)
			me.toClosed()
			return
		}
	}
}

// GetState returns current state of the worker
func (me *Worker) GetState() string {
	me.Lock()
	defer me.Unlock()
	return me.state
}

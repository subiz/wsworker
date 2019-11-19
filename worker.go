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

	sendqueue chan []byte

	commitChan chan<- int64
}

const BUFFER = 200

// NewWorker creates a new Worker object
func NewWorker(id string, commitChan chan<- int64) *Worker {
	w := &Worker{
		Mutex:          &sync.Mutex{},
		commitChan:     commitChan,
		Id:             id,
		state:          NORMAL,
		detached:       time.Now().Unix(),
		closeDeadline:  int64(DeadDeadline.Seconds()),
		commitDeadline: int64(OutdateDeadline.Seconds()),
		buffer:         make([]message, 0, 32),
		sendqueue:      make(chan []byte, BUFFER),
	}

	go w.sendLoop()
	return w
}

func (w *Worker) sendLoop() {
	for {
		if w.state == DEAD {
			return
		}

		for payload := range w.sendqueue {
			w.Lock()
			current_ws := w.ws
			w.Unlock()
			if err := w.ws.Send(payload); err != nil {
				log.Printf("[wsworker: %s] on send error %v", w.Id, err)
				w.Lock()
				if current_ws != w.ws {
					w.Unlock()
					break
				}

				// if still is my
				w.detachConnection()
				w.Unlock()
			}
		}
	}
}

// Stop forces worker to switch to DEAD mode
func (w *Worker) Stop() {
	w.Lock()
	defer w.Unlock()
	w.toDead()
}

func (w *Worker) trySend(payload []byte) error {
	select {
	case w.sendqueue <- payload:
		return nil
	default:
		return FULLERR
	}
}

// Commit returns commited offsets
func (me *Worker) doCommit() {
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
			// kill the worker if client don't commit in time
			firstmsg := me.buffer[0]
			timesincefirstsent := time.Now().Unix() - firstmsg.sent // second
			if timesincefirstsent > me.commitDeadline {
				log.Printf("[wsworker: %s] dead by late commit", me.Id)
				me.toDead()
			}
		}
		for _, offset := range committed_offsets {
			me.commitChan <- offset
		}
		return
	}
	me.buffer = nil
	for _, offset := range committed_offsets {
		me.commitChan <- offset
	}
}

// Attach attachs a websocket connection to the worker
// intro: the very first payload to send to client
func (me *Worker) Attach(r *http.Request, w http.ResponseWriter, intro []byte) error {
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
		me.trySend(intro)
	}

	// re-stream uncommitted messages to new connection
	for _, msg := range me.buffer {
		me.trySend(msg.payload)
	}

	// start the receive loop
	current_ws := me.ws
	go func() {
		me.Lock()
		// keep receive until current connection is detached
		for me.state == NORMAL && me.ws == current_ws && current_ws != nil {
			me.Unlock()
			p, err := current_ws.Recv()
			me.Lock()
			if me.ws != current_ws { // worker has detached this connection
				me.Unlock()
				return
			}

			if err != nil {
				log.Printf("[wsworker: %s] to error, normal recv %v", me.Id, err)
				me.detachConnection()
				me.Unlock()
				return
			}
			offset, _ := strconv.ParseInt(string(p), 10, 0)
			if offset < me.latest_committed_offset { // ignore invalid offset
				continue
			}
			me.latest_committed_offset = offset
			me.doCommit()
		}
		me.Unlock()
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
	if len(me.buffer) != 0 {
		// release all dirty messages
		for _, msg := range me.buffer {
			me.commitChan <- msg.offset
		}
	}
	me.buffer = nil
	close(me.sendqueue)
	me.sendqueue = nil
}

// detachConnection closes the current websocket connection and remove it
// out of worker
// Note: this function is not thread safe, caller should lock the worker before use
func (me *Worker) detachConnection() {
	if me.ws == nil {
		return
	}
	close(me.sendqueue)
	me.sendqueue = make(chan []byte, BUFFER)
	me.ws.Close()
	me.detached = time.Now().Unix()
	me.ws = nil
}

// Ping delivers a ping message to the client
// returns the current state of the worker
func (me *Worker) Ping() error {
	me.Lock()
	defer me.Unlock()

	if me.state == DEAD {
		return DEADERR
	}

	if me.ws == nil {
		// kill the worker if it has been dettached for too long
		now := time.Now().Unix() // second
		if now-me.detached < me.closeDeadline {
			return nil
		}
		me.toDead()
		return DEADERR
	}

	if err := me.ws.Ping(); err != nil {
		// client has gone away or there is something wrong with the websocket connection
		// we must dettach the current one
		log.Printf("[wsworker: %s] ping err %v", me.Id, err)
		me.detachConnection()
		return nil
	}
	return nil
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

	var msg = message{offset: offset, payload: payload, sent: now}
	me.buffer = append(me.buffer, msg)
	if len(me.buffer) > BUFFER {
		log.Printf("[wsworker: %s] dead by buffer overflow", me.Id)
		me.toDead()
		return DEADERR
	}

	if me.state == NORMAL && me.ws != nil {
		me.trySend(payload)
	}
	return nil
}

var (
	DEAD               = "dead"
	NORMAL             = "normal"
	DEADERR            = errors.New("dead")
	EMPTYCONNECTIONERR = errors.New("empty_connection")
	FULLERR            = errors.New("full")
)

const (
	OutdateDeadline = 2 * time.Minute
	DeadDeadline    = 2 * time.Minute
)

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
		sendqueue:      make(chan []byte, BUFFER+10),
	}

	return w
}

func (w *Worker) receiveLoop(currentWs Ws) {
	w.Lock()
	// keep receive until current connection is detached
	for w.state == NORMAL && w.ws == currentWs && currentWs != nil {
		w.Unlock()
		p, err := currentWs.Recv()
		w.Lock()
		if w.ws != currentWs { // worker has detached this connection
			w.Unlock()
			return
		}

		if err != nil {
			log.Printf("[wsworker: %s] to error, normal recv %v", w.Id, err)
			w.detachConnection()
			w.Unlock()
			return
		}
		offset, _ := strconv.ParseInt(string(p), 10, 0)
		if offset < w.latest_committed_offset { // ignore invalid offset
			continue
		}
		w.latest_committed_offset = offset
		w.doCommit()
	}
	w.Unlock()
}

func (w *Worker) sendLoop(sendqueue chan []byte) {
	for payload := range sendqueue {
		if payload == nil { // exit signal
			return
		}

		w.Lock()
		currentWs := w.ws
		if currentWs == nil {
			w.Unlock()
			continue
		}
		w.Unlock()

		err := currentWs.Send(payload)

		w.Lock()
		if currentWs != w.ws { // world is changed, new connection is attached
			// clean up
			w.Unlock()
			return
		}

		if err == nil {
			w.Unlock()
			continue
		}

		log.Printf("[wsworker: %s] on send error %v", w.Id, err)

		// if still is my
		w.detachConnection()
		w.Unlock()
		return
	}
}

// Stop forces worker to switch to DEAD mode
func (w *Worker) Stop() {
	w.Lock()
	defer w.Unlock()
	w.toDead()
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
	me.sendqueue = make(chan []byte, BUFFER+10)
	go me.sendLoop(me.sendqueue) // start send loop
	go me.receiveLoop(ws)

	// can't be blocked since sendqueue has way more free space for me.buffer
	if intro != nil {
		me.sendqueue <- intro // could panic
	}
	// re-stream uncommitted messages to new connection
	for _, msg := range me.buffer {
		me.sendqueue <- msg.payload
	}
	return nil
}

func flush(sendqueue chan []byte) {
	out := false
	for {
		select {
		case <-sendqueue:
		default:
			out = true
		}
		if out {
			break
		}
	}
	if sendqueue == nil {
		return
	}
	sendqueue <- nil // send end signal
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
	me.ws = nil
	flush(me.sendqueue)
	log.Printf("[wsworker: %s] onDead22", me.Id)
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
	flush(me.sendqueue)
	me.ws = nil // will flush sendqueue
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
		me.detachConnection()
	}
	return nil
}

// Send delivers a message to the client
// ignore the request if the worker is DEAD
func (me *Worker) Send(offset int64, payload []byte) {
	me.Lock()
	defer me.Unlock()

	// ignore invalid payload
	if payload == nil {
		me.commitChan <- offset
		return
	}

	if me.state == DEAD {
		me.commitChan <- offset
		return
	}
	now := time.Now().Unix()

	var msg = message{offset: offset, payload: payload, sent: now}
	me.buffer = append(me.buffer, msg)
	if len(me.buffer) > BUFFER {
		log.Printf("[wsworker: %s] dead by buffer overflow", me.Id)
		me.toDead()
		return
	}

	if me.ws != nil {
		me.sendqueue <- payload
	}
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

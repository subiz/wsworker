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
	IsClosed() bool
	Close() error
	Recv() ([]byte, error)
	Ping() error
	Send(data []byte) error // do not panic //TODO: send to queue
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

	buffer []message

	commitChan chan<- int64
}

// NewWorker creates a new Worker object
func NewWorker(id string, commitChan chan<- int64) *Worker {
	w := &Worker{
		Mutex:      &sync.Mutex{},
		commitChan: commitChan,
		Id:         id,
		state:      NORMAL,
		detached:   time.Now().Unix(),
		buffer:     make([]message, 0, BUFFER),
	}

	return w
}

func (w *Worker) receiveLoop(ws Ws) {
	w.Lock()
	// keep receive until current connection is detached
	for w.state == NORMAL && !ws.IsClosed() {
		w.Unlock()
		p, err := ws.Recv()
		w.Lock()

		if ws.IsClosed() {
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

// Stop forces worker to switch to DEAD mode
func (w *Worker) Stop() {
	w.Lock()
	defer w.Unlock()
	w.toDead("stop")
}

// Commit returns commited offsets
func (me *Worker) doCommit() {
	var firstUncommitedOffset int
	for i := range me.buffer {
		if me.buffer[i].offset <= me.latest_committed_offset {
			firstUncommitedOffset = i + 1
			me.commitChan <- me.buffer[i].offset
			continue
		}
		break
	}

	// remove committed offsets in buffer
	if firstUncommitedOffset > 0 {
		me.buffer = me.buffer[firstUncommitedOffset:]
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
	go me.receiveLoop(ws)

	// can't be blocked since sendqueue has way more free space for me.buffer
	if intro != nil {
		ws.Send(intro)
	}
	// re-stream uncommitted messages to new connection
	for _, msg := range me.buffer {
		ws.Send(msg.payload)
	}
	return nil
}

// toDead switchs worker to DEAD mode, release all holding resources
// once worker is dead, it cannot be reused. User should not Send message on dead ws
// Note: this function is not thread safe, caller should lock the worker before use
func (me *Worker) toDead(reason string) {
	if me.state == DEAD {
		return
	}

	log.Printf("[wsworker: %s] DEAD cuz %s", me.Id, reason)

	me.state = DEAD
	me.detachConnection()
	if len(me.buffer) != 0 {
		// release all dirty messages
		for _, msg := range me.buffer {
			me.commitChan <- msg.offset
		}
	}
	me.buffer = nil
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

// AreYouAlive delivers a ping message to the client
// returns the current state of the worker
func (me *Worker) AreYouAlive() error {
	me.Lock()
	defer me.Unlock()

	if me.state == DEAD {
		return DEADERR
	}

	// kill the worker if it has been dettached for too long
	if me.ws == nil {
		now := time.Now().Unix() // second
		if now-me.detached < int64(DeadDeadline.Seconds()) {
			return nil
		}
		me.toDead("dettached too long")
		return DEADERR
	}

	// kill the worker if client don't commit in time
	if len(me.buffer) > 0 {
		firstmsg := me.buffer[0]
		timesincefirstsent := time.Now().Unix() - firstmsg.sent // second
		if timesincefirstsent > int64(OutdateDeadline.Seconds()) {
			me.toDead("late commit")
			return DEADERR
		}
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
		me.toDead("overflow")
		return
	}
	me.ws.Send(payload)
}

var (
	DEADERR            = errors.New("dead")
	EMPTYCONNECTIONERR = errors.New("empty_connection")
	FULLERR            = errors.New("full")
)

const (
	DEAD   = "dead"
	NORMAL = "normal"

	// duration in which worker must received commit message after send a message to client
	// if commit message come too late, worker will be killed
	OutdateDeadline = 2 * time.Minute

	// duration in which worker must be reattached after detached
	// if worker have been detached too long, it will be killed
	DeadDeadline = 2 * time.Minute

	// maximum number of uncommitted messages
	// if number of messages execess this, worker will be killed
	BUFFER = 200
)

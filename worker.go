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
	CLOSED       = "closed"
	DEAD         = "dead"
	NORMAL       = "normal"
	REPLAY       = "replay"
	DEADERR      = errors.New("dead")
	REPLAYINGERR = errors.New("replaying")
)

type Ws interface {
	IsClosed() bool
	Close() error
	Recv() ([]byte, error)
	Ping() error
	Send(data []byte) error
}

type message struct {
	Offset  int64
	Payload []byte
}

type Commit struct {
	Id     string
	Offset int64
}

type Worker struct {
	*sync.Mutex
	Id          string
	offset      int64
	dirty       bool      // have message that have not been committed
	closed      time.Time // last closed time
	committed   time.Time // last closed time
	ws          Ws
	state       string
	deadChan    chan<- string
	commitChan  chan<- Commit
	replayQueue []*message
}

func NewWorker(id string, deadChan chan<- string, commitChan chan<- Commit) *Worker {
	return &Worker{
		Mutex:       &sync.Mutex{},
		Id:          id,
		state:       CLOSED,
		closed:      time.Now(),
		committed:   time.Now(),
		deadChan:    deadChan,
		commitChan:  commitChan,
		replayQueue: make([]*message, 0, 32),
	}
}

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
	case CLOSED:
		me.ws = ws
		if intro != nil {
			me.ws.Send(intro)
			// ignore err because we will eventually find out when replay
		}
		me.toReplay()
	case NORMAL:
		log.Printf("[wsworker: %s] new on normal", me.Id)
		me.toClosed()

		me.ws = ws
		if intro != nil {
			me.ws.Send(intro)
			// ignore err because we will eventually find out when replay
		}
		me.toReplay()
	case REPLAY:
		return REPLAYINGERR
	case DEAD:
		return DEADERR
	}
	return nil
}

// chop queue, return new queue and the first offset
func chop(queue []*message, offset int64) []*message {
	for i, msg := range queue {
		if offset == msg.Offset {
			return queue[i+1:]
		}
	}
	return queue
}

func (me *Worker) toNormal() {
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
			me.offset, _ = strconv.ParseInt(string(p), 10, 0)
			me.dirty = true
		}
	}()
}

func (w *Worker) onNormalMsg(msg *message) {
	if msg == nil || w.ws == nil {
		w.toDead()
		return
	}
	w.replayQueue = append(w.replayQueue, msg)
	if len(w.replayQueue) > 2000 {
		log.Printf("[wsworker: %s] dead by replay queue size", w.Id)
		w.toDead()
	}

	defer func() {
		if r := recover(); r != nil {
			w.toDead()
		}
	}()
	if err := w.ws.Send(msg.Payload); err != nil {
		log.Printf("[wsworker: %s] on send error %v", w.Id, err)
		w.toClosed()
		return
	}
}

func (w *Worker) toReplay() {
	w.state = REPLAY
	for _, m := range w.replayQueue {
		if err := w.ws.Send(m.Payload); err != nil {
			log.Printf("[wsworker: %s] on send error %v", w.Id, err)
			w.toClosed()
			return
		}
	}
	w.toNormal()
}

func (w *Worker) toClosed() {
	w.state = CLOSED
	w.closed = time.Now()
	log.Printf("[wsworker: %s] CLOSING", w.Id)
	if w.ws != nil {
		w.ws.Close()
		w.ws = nil
	}
}

// state = closed
func (w *Worker) DeadCheck(deadline time.Duration) {
	w.Lock()
	defer w.Unlock()

	if w.state != CLOSED {
		return
	}

	if time.Since(w.closed) < deadline {
		return
	}
	log.Printf("[wsworker: %s] dead by close check %v", w.Id, time.Since(w.closed))
	w.toDead()
}

func (w *Worker) toDead() {
	log.Printf("[wsworker: %s] onDead", w.Id)
	// release resource
	w.state = DEAD
	if w.ws != nil {
		w.ws.Close()
		w.ws = nil
	}
	if len(w.replayQueue) != 0 { // commit the last message
		lastmsg := w.replayQueue[len(w.replayQueue)-1]
		w.commitChan <- Commit{Id: w.Id, Offset: lastmsg.Offset}
		w.replayQueue = nil
	}
	w.deadChan <- w.Id
}

func (w *Worker) PingCheck() {
	w.Lock()
	defer w.Unlock()

	if w.state != NORMAL {
		return
	}

	if err := w.ws.Ping(); err != nil {
		log.Printf("[wsworker: %s] ping err %v", w.Id, err)
		w.toClosed()
		return
	}
}

func (w *Worker) CommitCheck() {
	w.Lock()
	defer w.Unlock()

	if w.state != NORMAL {
		return
	}

	if !w.dirty {
		return
	}

	newqueue := chop(w.replayQueue, w.offset)
	if len(newqueue) != len(w.replayQueue) {
		w.commitChan <- Commit{Id: w.Id, Offset: w.offset}
	}
	w.dirty, w.replayQueue, w.committed = false, newqueue, time.Now()
}

func (w *Worker) OutdateCheck(deadline time.Duration) {
	w.Lock()
	defer w.Unlock()

	if w.state != NORMAL {
		return
	}

	if deadline < time.Since(w.committed) && 0 < len(w.replayQueue) {
		log.Printf("[wsworker: %s] dead by replay queue time, %d, %v", w.Id, len(w.replayQueue), time.Since(w.committed))
		w.toDead()
		return
	}
}

// Send
func (w *Worker) Send(msg *message) {
	w.Lock()
	defer w.Unlock()

	switch w.state {
	case NORMAL:
		w.onNormalMsg(msg)
	case CLOSED:
		w.replayQueue = append(w.replayQueue, msg)
		if len(w.replayQueue) > 2000 {
			w.toDead()
		}
	case REPLAY:
		panic("by design, this should not happendedbool")
	case DEAD:
		w.commitChan <- Commit{Id: w.Id, Offset: msg.Offset}
	}
}

// GetState returns current state of the worker
func (w *Worker) GetState() string {
	w.Lock()
	defer w.Unlock()
	return w.state
}

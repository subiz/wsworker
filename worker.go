package wsworker

import (
	"bitbucket.org/subiz/wsworker/driver/gorilla"
	"errors"
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

type Message struct {
	Id      string
	Offset  int64
	Payload []byte
}

type Commit struct {
	Id     string
	Offset int64
}

type Worker struct {
	*sync.Mutex
	id          string
	offset      int64
	dirty       bool      // have message that have not been committed
	pinged      time.Time // last pinged time
	closed      time.Time // last closed time
	committed   time.Time // last committed time
	ws          Ws
	state       string
	deadChan    chan<- string
	commitChan  chan<- Commit
	replayQueue []*Message
}

func NewWorker(id string, deadChan chan<- string, commitChan chan<- Commit) *Worker {
	return &Worker{
		Mutex:       &sync.Mutex{},
		id:          id,
		state:       CLOSED,
		pinged:      time.Now(),
		closed:      time.Now(),
		committed:   time.Now(),
		deadChan:    deadChan,
		commitChan:  commitChan,
		replayQueue: []*Message{},
	}
}

func (w *Worker) recvLoop(ws Ws) {
	w.Lock()
	for w.state == NORMAL && !ws.IsClosed() {
		w.Unlock()
		p, err := ws.Recv()
		w.Lock()
		w.onNormalRecv(p, err)
	}
	w.Unlock()
}

func (me *Worker) SetConnection(r *http.Request, w http.ResponseWriter) error {
	me.Lock()
	defer me.Unlock()

	ws, err := gorilla.NewWs(w, r, nil)
	if err != nil {
		return err
	}
	switch me.state {
	case CLOSED:
		me.onClosedNewConn(ws)
	case NORMAL:
		me.onNormalNewConn(ws)
	case REPLAY:
		return REPLAYINGERR
	case DEAD:
		return DEADERR
	}
	return nil
}

// chop queue, return new queue and the first offset
func chop(queue []*Message, offset int64) []*Message {
	for i, msg := range queue {
		if offset == msg.Offset {
			return queue[i:]
		}
	}
	return queue
}

func (w *Worker) toNormal() {
	w.state = NORMAL
	go w.recvLoop(w.ws)
}

func (w *Worker) onNormalPingCheck(deadline time.Duration) {
	if time.Since(w.pinged) < deadline {
		return
	}
	if err := w.ws.Ping(); err != nil {
		w.toClosed()
		return
	}
	w.pinged = time.Now()
}

func (w *Worker) onNormalOutdateCheck(deadline time.Duration) {
	if deadline < time.Since(w.committed) && 0 < len(w.replayQueue) {
		w.toDead()
		return
	}
}

func (w *Worker) onNormalCommitCheck() {
	if !w.dirty {
		return
	}

	newqueue := chop(w.replayQueue, w.offset)
	if len(newqueue) != len(w.replayQueue) {
		w.commitChan <- Commit{Id: w.id, Offset: w.offset}
	}
	w.dirty, w.replayQueue, w.committed = false, newqueue, time.Now()
}

func (w *Worker) onNormalMsg(msg *Message) {
	w.replayQueue = append(w.replayQueue, msg)
	if err := w.ws.Send(msg.Payload); err != nil {
		log.Printf("[wsworker: %s] on send error %v", w.id, err)
		w.toClosed()
		return
	}
}

func (w *Worker) onNormalRecv(p []byte, err error) {
	if err != nil {
		w.toClosed()
		return
	}
	w.offset, w.dirty = strToInt(string(p)), true
}

func (w *Worker) onReplayMsg(msg *Message) {
	w.replayQueue = append(w.replayQueue, msg)
}

func (w *Worker) toReplay() {
	w.state = REPLAY

	for _, m := range w.replayQueue {
		if err := w.ws.Send(m.Payload); err != nil {
			log.Printf("[wsworker: %s] on send error %v", w.id, err)
			w.toClosed()
			return
		}
		w.Unlock()
		w.Lock()
	}
	w.toNormal()
}

func (w *Worker) toClosed() {
	w.state = CLOSED
	w.closed = time.Now()
	w.ws.Close()
}

func (w *Worker) onNormalNewConn(newws Ws) {
	w.toClosed()
	w.onClosedNewConn(newws)
}

func (w *Worker) onClosedNewConn(newws Ws) {
	w.ws = newws
	w.toReplay()
}

func (w *Worker) onClosedMsg(msg *Message) {
	w.replayQueue = append(w.replayQueue, msg)
}

func (w *Worker) onClosedDeadCheck(deadline time.Duration) {
	if time.Since(w.closed) < deadline {
		return
	}
	w.toDead()
}

// state = closed
func (w *Worker) DeadCheck(deadline time.Duration) {
	w.Lock()
	defer w.Unlock()

	if w.state != CLOSED {
		return
	}

	w.onClosedDeadCheck(deadline)
}

func (w *Worker) toDead() {
	log.Printf("[wsworker: %s] onDead", w.id)
	// release resource
	w.state = DEAD
	w.ws.Close()
	w.replayQueue = nil
	w.deadChan <- w.id
}

func strToInt(str string) int64 {
	i, _ := strconv.ParseInt(str, 10, 0)
	return i
}

func (w *Worker) PingCheck(deadline time.Duration) {
	w.Lock()
	defer w.Unlock()

	if w.state != NORMAL {
		return
	}

	w.onNormalPingCheck(deadline)
}

func (w *Worker) DieCheck(deadline time.Duration) {
	w.Lock()
	defer w.Unlock()

	if w.state != CLOSED {
		return
	}

	w.onClosedDeadCheck(deadline)
}

func (w *Worker) CommitCheck() {
	w.Lock()
	defer w.Unlock()

	if w.state != NORMAL {
		return
	}

	w.onNormalCommitCheck()
}

func (w *Worker) OutdateCheck(deadline time.Duration) {
	w.Lock()
	defer w.Unlock()

	if w.state != NORMAL {
		return
	}

	w.onNormalOutdateCheck(deadline)
}

func (w *Worker) Send(msg *Message) {
	w.Lock()
	defer w.Unlock()
	switch w.state {
	case NORMAL:
		w.onNormalMsg(msg)
	case CLOSED:
		w.onClosedMsg(msg)
	case REPLAY:
		w.onReplayMsg(msg)
	case DEAD:
		return
	}
}

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
	id          string
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
		id:          id,
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

func (w *Worker) recvLoop(ws Ws) {
	w.Lock()
	defer w.Unlock()

	for w.state == NORMAL && !ws.IsClosed() {
		w.Unlock()
		p, err := ws.Recv()
		w.Lock()
		if ws != w.ws {
			return
		}
		w.onNormalRecv(p, err)
	}
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
		me.onClosedNewConn(ws, intro)
	case NORMAL:
		me.onNormalNewConn(ws, intro)
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

func (w *Worker) toNormal() {
	w.state = NORMAL
	go w.recvLoop(w.ws)
}

func (w *Worker) onNormalPingCheck() {
	if err := w.ws.Ping(); err != nil {
		log.Printf("[wsworker: %s] ping err %v", w.id, err)
		w.toClosed()
		return
	}
}

func (w *Worker) onNormalOutdateCheck(deadline time.Duration) {
	if deadline < time.Since(w.committed) && 0 < len(w.replayQueue) {
		log.Printf("[wsworker: %s] dead by replay queue time", w.id)
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

func (w *Worker) onNormalMsg(msg *message) {
	if msg == nil || w.ws == nil {
		w.toDead()
		return
	}
	w.replayQueue = append(w.replayQueue, msg)
	if len(w.replayQueue) > 2000 {
		log.Printf("[wsworker: %s] dead by replay queue size", w.id)
		w.toDead()
	}

	defer func() {
		if r := recover(); r != nil {
			w.toDead()
		}
	}()
	if err := w.ws.Send(msg.Payload); err != nil {
		log.Printf("[wsworker: %s] on send error %v", w.id, err)
		w.toClosed()
		return
	}
}

func (w *Worker) onNormalRecv(p []byte, err error) {
	if err != nil {
		log.Printf("[wsworker: %s] to error, normal recv %v", w.id, err)
		w.toClosed()
		return
	}
	w.offset, w.dirty = strToInt(string(p)), true
}

func (w *Worker) toReplay() {
	w.state = REPLAY
	for _, m := range w.replayQueue {
		if err := w.ws.Send(m.Payload); err != nil {
			log.Printf("[wsworker: %s] on send error %v", w.id, err)
			w.toClosed()
			return
		}
	}
	w.toNormal()
}

func (w *Worker) toClosed() {
	w.state = CLOSED
	w.closed = time.Now()
	log.Printf("[wsworker: %s] CLOSING", w.id)
	if w.ws != nil {
		w.ws.Close()
		w.ws = nil
	}
}

func (w *Worker) onNormalNewConn(newws Ws, intro []byte) {
	log.Printf("[wsworker: %s] new on normal", w.id)
	w.toClosed()
	w.onClosedNewConn(newws, intro)
}

func (w *Worker) onClosedNewConn(newws Ws, intro []byte) {
	w.ws = newws
	if intro != nil {
		w.ws.Send(intro)
		// ignore err because we will eventually find out when replay
	}
	w.toReplay()
}

func (w *Worker) onClosedMsg(msg *message) {
	w.replayQueue = append(w.replayQueue, msg)
	if len(w.replayQueue) > 2000 {
		w.toDead()
	}
}

func (w *Worker) onClosedDeadCheck(deadline time.Duration) {
	if time.Since(w.closed) < deadline {
		return
	}
	log.Printf("[wsworker: %s] dead by close check", w.id)
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
	if w.ws != nil {
		w.ws.Close()
		w.ws = nil
	}
	if len(w.replayQueue) != 0 { // commit the last message
		lastmsg := w.replayQueue[len(w.replayQueue)-1]
		w.commitChan <- Commit{Id: w.id, Offset: lastmsg.Offset}
		w.replayQueue = nil
	}
	w.deadChan <- w.id
}

func strToInt(str string) int64 {
	i, _ := strconv.ParseInt(str, 10, 0)
	return i
}

func (w *Worker) PingCheck() {
	w.Lock()
	defer w.Unlock()

	if w.state != NORMAL {
		return
	}

	w.onNormalPingCheck()
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

func (w *Worker) onDeadMsg(msg *message) {
	w.commitChan <- Commit{Id: w.id, Offset: msg.Offset}
}

func (w *Worker) Send(msg *message) {
	w.Lock()
	defer w.Unlock()
	switch w.state {
	case NORMAL:
		w.onNormalMsg(msg)
	case CLOSED:
		w.onClosedMsg(msg)
	case REPLAY:
		panic("by design, this should not happendedbool")
	case DEAD:
		w.onDeadMsg(msg)
	}
}

func (w *Worker) GetState() string {
	w.Lock()
	defer w.Unlock()
	return w.state
}

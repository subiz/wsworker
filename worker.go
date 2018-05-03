package wsworker

import (
	"errors"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	CLOSED = "closed"
	DEAD   = "dead"
	NORMAL = "normal"
	REPLAY = "replay"
)

var closeTimeout = 5 * time.Minute
var commitTimeout = 30 * time.Minute

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

type IWorker interface {
	SetConnection(req *http.Request, res http.ResponseWriter) error
	Debug()
	TotalInQueue() int
}

type Id = string

type Message struct {
	Id      Id
	Offset  int
	Payload []byte
}

type Commit struct {
	Id     Id
	Offset int
}

type Worker struct {
	sync.RWMutex

	Id         Id
	msgChan    <-chan Message
	deadChan   chan<- Id
	commitChan chan<- Commit

	ws             *websocket.Conn
	state          string
	replayQueue    []*Message
	commitOffset   int
	stopChan       chan bool
	lastCloseTime  time.Time
	lastCommitTime time.Time
}

func NewWorker(id Id, msgChan <-chan Message, deadChan chan<- Id, commitChan chan<- Commit) IWorker {
	return &Worker{
		Id:          id,
		msgChan:     msgChan,
		deadChan:    deadChan,
		commitChan:  commitChan,
		replayQueue: []*Message{},
		stopChan:    make(chan bool),
	}
}

func (me *Worker) SetConnection(r *http.Request, w http.ResponseWriter) error {
	log.Printf("[wsworker: %s] SetConnection", me.Id)

	ws, err := upgradeHandler(w, r)
	if err != nil {
		return err
	}

	go me.wsOnMessage(ws)

	if me.state == DEAD {
		return errors.New("dead")
	}

	if me.state == REPLAY {
		return errors.New("replaying")
	}

	if me.state == NORMAL {
		me.Stop()
	}

	// state = "" OR state = CLOSED
	me.ws = ws
	me.lastCommitTime = time.Now()
	me.lastCloseTime = time.Now()
	me.replay()

	if me.state == NORMAL {
		go me.deadChecker()
		go me.chopReplayQueue()
		go me.send()
	}

	return nil
}

func (me *Worker) send() {
	log.Printf("[wsworker: %s] send", me.Id)
loop:
	for {
		select {
		case msg := <-me.msgChan:
			me.Lock()
			me.replayQueue = append(me.replayQueue, &msg)
			me.Unlock()

			err := me.wsSend(msg.Payload)
			if err != nil {
				break loop
			}

			log.Printf("[wsworker: %s] sent: %s", me.Id, string(msg.Payload))
		case <-me.stopChan:
			break loop
		}
	}

	me.setState(CLOSED)
}

func (me *Worker) wsSend(payload []byte) error {
	return me.ws.WriteMessage(websocket.TextMessage, payload)
}

func (me *Worker) replay() {
	log.Printf("[wsworker: %s] replay", me.Id)
	log.Printf("[wsworker: %s] total message in replay queue: %d", me.Id, len(me.replayQueue))
	me.setState(REPLAY)

	for _, m := range me.replayQueue {
		err := me.wsSend(m.Payload)
		if err != nil {
			me.setState(CLOSED)
			return
		}
	}

	me.setState(NORMAL)
}

// no websocket connection for 5 minutes
// OR dont see "commit" for a long time (30 minutes)
func (me *Worker) deadChecker() {
	log.Printf("[wsworker: %s] deadChecker", me.Id)
	if me.state == DEAD {
		return
	}

	ticker := time.NewTicker(1 * time.Second)

	for range ticker.C {
		closeDuration := time.Now().Sub(me.lastCloseTime)
		if me.ws == nil && closeDuration > closeTimeout {
			break
		}

		commitDuration := time.Now().Sub(me.lastCommitTime)
		if commitDuration > commitTimeout {
			break
		}
	}

	go func() {
		me.deadChan <- me.Id
	}()

	me.setState(DEAD)
	me.Stop()
}

func (me *Worker) waitStopped() {
	log.Printf("[wsworker: %s] waitStopped", me.Id)
	for me.state != CLOSED && me.state != DEAD {
		time.Sleep(100 * time.Millisecond)
	}
}

func (me *Worker) wsOnMessage(ws *websocket.Conn) {
	log.Printf("[wsworker: %s] wsOnMessage", me.Id)
loop:
	for {
		_, p, err := ws.ReadMessage()
		if err != nil {
			log.Printf("[wsworker: %s] read message error: %s", me.Id, err)
			me.Stop()
			break loop
		}

		offset := strToInt(string(p))

		if offset > me.commitOffset {
			me.commitOffset = offset
		}
	}
}

func (me *Worker) chopReplayQueue() {
	log.Printf("[wsworker: %s] chopReplayQueue", me.Id)
	ticker := time.NewTicker(1 * time.Second)

	for range ticker.C {
		go func() {
			me.commitChan <- Commit{
				Id:     me.Id,
				Offset: me.commitOffset,
			}
		}()

		me.Lock()

		newReplayQueue := []*Message{}

		for _, msg := range me.replayQueue {
			if msg.Offset > me.commitOffset {
				newReplayQueue = append(newReplayQueue, msg)
			}
		}

		me.replayQueue = newReplayQueue
		me.Unlock()
	}
}

func (me *Worker) Debug() {
	log.Println("-------------------------------------------------")
	log.Printf("[wsworker: %s] state: %s\n", me.Id, me.state)
	log.Printf("[wsworker: %s] replayQueue: %v\n", me.Id, me.replayQueue)
	log.Printf("[wsworker: %s] commitOffset: %d\n", me.Id, me.commitOffset)
	log.Printf("[wsworker: %s] lastCloseTime: %s\n", me.Id, me.lastCloseTime)
	log.Printf("[wsworker: %s] lastCommitTime: %s\n", me.Id, me.lastCommitTime)
}

func (me *Worker) setState(state string) {
	log.Printf("[wsworker: %s] setState: %s", me.Id, state)
	if me.state == DEAD {
		return
	}

	me.state = state
}

func (me *Worker) Stop() {
	log.Printf("[wsworker: %s] Stop", me.Id)
	me.stopChan <- true
	me.lastCloseTime = time.Now()

	if me.ws != nil {
		me.ws.Close()
		me.ws = nil
	}

	me.waitStopped()
}

func (me *Worker) TotalInQueue() int {
	return len(me.replayQueue)
}

func upgradeHandler(w http.ResponseWriter, r *http.Request) (*websocket.Conn, error) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return nil, err
	}

	return ws, nil
}

func strToInt(str string) int {
	i, _ := strconv.Atoi(str)
	return i
}

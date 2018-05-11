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

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

type IWorker interface {
	SetConnection(req *http.Request, res http.ResponseWriter) error
	TotalInQueue() int
	GetState() string
	SetConfig(config Config)
	SwitchState(string)
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

type Config struct {
	closeTimeout  time.Duration
	commitTimeout time.Duration
}

type State struct {
	state string
	done  chan bool
}

type Worker struct {
	sync.Mutex

	id         Id
	chopChan   chan int
	msgChan    <-chan Message
	deadChan   chan<- Id
	commitChan chan<- Commit

	config      *Config
	ws          *websocket.Conn
	state       string
	replayQueue []*Message

	lastCloseTime  time.Time
	lastCommitTime time.Time

	stateChan chan State

	stopDeadChecker chan bool
	stopReplay      bool
}

func NewWorker(id Id, msgChan <-chan Message, deadChan chan<- Id, commitChan chan<- Commit) IWorker {
	w := &Worker{
		id:          id,
		msgChan:     msgChan,
		deadChan:    deadChan,
		commitChan:  commitChan,
		replayQueue: []*Message{},
		config: &Config{
			closeTimeout:  5 * time.Minute,
			commitTimeout: 30 * time.Minute,
		},
		stopReplay:      false,
		stopDeadChecker: make(chan bool),
		chopChan:        make(chan int),
		state:           CLOSED,
		stateChan:       make(chan State),
	}

	go w.stateSwitcher()
	go w.deadChecker()

	return w
}

func (me *Worker) SetConnection(r *http.Request, w http.ResponseWriter) error {
	log.Printf("[wsworker: %s] SetConnection", me.id)

	me.Lock()
	defer me.Unlock()

	// deny
	if me.state != NORMAL && me.state != CLOSED {
		log.Printf("[wsworker: %s] SetConnection - not allow", me.id)
		return errors.New("not allow")
	}

	me.SwitchState(CLOSED) // release resources
	me.handleWebsocket(w, r)

	me.SwitchState(REPLAY)
	me.SwitchState(NORMAL)

	return nil
}

func (me *Worker) SwitchState(state string) {
	log.Printf("[wsworker: %s] SwitchState: %s", me.id, state)

	done := make(chan bool)
	me.stateChan <- State{state: state, done: done}
	<-done
}

func (me *Worker) stateSwitcher() {
	log.Printf("[wsworker: %s] stateSwitcher", me.id)

	for s := range me.stateChan {
		state := s.state
		log.Printf("[wsworker: %s] %s -> %s", me.id, me.state, state)

		if state == me.state {
			s.done <- true
			continue
		}

		switch state {
		case NORMAL:
			if me.state == REPLAY {
				me.state = NORMAL
				go me.onNormal()
			}

		case REPLAY:
			if me.state == CLOSED {
				me.state = REPLAY
				me.onReplay()
			}

		case CLOSED:
			if me.state == NORMAL || me.state == REPLAY {
				me.state = CLOSED
				me.onClosed()
			}

		case DEAD:
			if me.state == NORMAL || me.state == CLOSED {
				me.state = DEAD
				me.onDead()
			}
		}

		s.done <- true
	}
}

func chop(queue []*Message, offset int) []*Message {
	for i, msg := range queue {
		if offset < msg.Offset {
			return queue[i:]
		}
	}
	return nil
}

func (me *Worker) onNormal() {
	log.Printf("[wsworker: %s] onNormal", me.id)
	var offset = -1
	for {
		select {
		case offset = <-me.chopChan:
		case <-time.After(1 * time.Second):
			if offset == -1 {
				continue
			}
			me.commitChan <- Commit{Id: me.id, Offset: offset}
			me.replayQueue = chop(me.replayQueue, offset)
			offset = -1

		case msg := <-me.msgChan:
			me.replayQueue = append(me.replayQueue, &msg)
			if err := me.wsSend(msg.Payload); err != nil {
				go me.SwitchState(CLOSED)
				return
			}
		}
	}
}

func (me *Worker) onReplay() {
	log.Printf("[wsworker: %s] onReplay", me.id)
	log.Printf("[wsworker: %s] total message in queue: %d", me.id, len(me.replayQueue))

	me.stopReplay = false

	for _, m := range me.replayQueue {
		err := me.wsSend(m.Payload)
		if err != nil {
			// use goroutine to fix blocking: SwitchState (wait onReplay - onReplay wait SwitchState)
			go func() {
				me.SwitchState(CLOSED)
			}()

			break
		}

		log.Printf("[wsworker: %s] replayed: %s", me.id, string(m.Payload))

		if me.stopReplay {
			break
		}
	}
}

func (me *Worker) onClosed() {
	log.Printf("[wsworker: %s] onClosed", me.id)

	me.lastCloseTime = time.Now()

	me.stopReplay = true

}

func (me *Worker) onDead() {
	log.Printf("[wsworker: %s] onDead", me.id)

	// release resource
	if me.ws != nil {
		me.ws.Close()
	}
	me.ws = nil
	me.replayQueue = nil

	me.deadChan <- me.id
}

// no websocket connection for 5 minutes
// OR dont see "commit" for a long time (30 minutes)
func (me *Worker) deadChecker() {
	log.Printf("[wsworker: %s] deadChecker", me.id)

	ticker := time.NewTicker(1 * time.Second)
loop:
	for {
		select {
		case <-ticker.C:
			log.Printf("[wsworker: %s] deadChecker checking", me.id)
			// me.debug()

			if me.state != CLOSED && me.state != NORMAL {
				return
			}

			closeDuration := time.Now().Sub(me.lastCloseTime)
			if me.state == CLOSED && closeDuration > me.config.closeTimeout {
				log.Printf("[wsworker: %s] close timeout", me.id)
				me.SwitchState(DEAD)
			}

			commitDuration := time.Now().Sub(me.lastCommitTime)
			if me.state == NORMAL && commitDuration > me.config.commitTimeout {
				log.Printf("[wsworker: %s] commit timeout", me.id)
				me.SwitchState(DEAD)
			}

		case <-me.stopDeadChecker:
			break loop
		}
	}
}

func (me *Worker) TotalInQueue() int {
	return len(me.replayQueue)
}

func (me *Worker) GetState() string {
	return me.state
}

func (me *Worker) SetConfig(config Config) {
	me.config = &config
}

func (me *Worker) handleWebsocket(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("handleWebsocket error: %s", err)
		return
	}

	me.ws = ws
	me.lastCommitTime = time.Now()
	me.lastCloseTime = time.Now()

	go me.wsOnMessage(ws)
}

func (me *Worker) wsSend(payload []byte) error {
	if me.ws == nil {
		return nil
	}

	return me.ws.WriteMessage(websocket.TextMessage, payload)
}

func (me *Worker) wsOnMessage(ws *websocket.Conn) {
	log.Printf("[wsworker: %s] wsOnMessage", me.id)
	currentoffset := 0
	for {
		_, p, err := ws.ReadMessage()
		if err != nil {
			log.Printf("[wsworker: %s] read message error: %s", me.id, err)
			me.SwitchState(CLOSED)
			break
		}

		offset := strToInt(string(p))
		if offset > currentoffset {
			me.chopChan <- offset
			currentoffset = offset
		}

		me.lastCommitTime = time.Now()
	}
}

func (me *Worker) debug() {
	log.Println("-------------------------------------------------")
	log.Printf("[wsworker: %s] state: %s\n", me.id, me.state)
	log.Printf("[wsworker: %s] replayQueue: %v\n", me.id, me.replayQueue)
	log.Printf("[wsworker: %s] lastCloseTime: %s\n", me.id, me.lastCloseTime)
	log.Printf("[wsworker: %s] lastCommitTime: %s\n", me.id, me.lastCommitTime)
}

func strToInt(str string) int {
	i, _ := strconv.Atoi(str)
	return i
}

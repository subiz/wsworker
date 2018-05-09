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
	msgChan    <-chan Message
	deadChan   chan<- Id
	commitChan chan<- Commit

	config       *Config
	ws           *websocket.Conn
	state        string
	replayQueue  []*Message
	commitOffset int

	lastCloseTime  time.Time
	lastCommitTime time.Time

	stateChan chan State

	stopDeadChecker     chan bool
	stopSend            chan bool
	stopChopReplayQueue chan bool
	stopReplay          bool
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
		stopSend:            make(chan bool),
		stopReplay:          false,
		stopChopReplayQueue: make(chan bool),
		stopDeadChecker:     make(chan bool),
		state:               CLOSED,
		stateChan:           make(chan State),
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

func (me *Worker) onNormal() {
	log.Printf("[wsworker: %s] onNormal", me.id)

	go me.chopReplayQueue()

loop:
	for {
		select {
		case msg := <-me.msgChan:
			me.replayQueue = append(me.replayQueue, &msg)

			err := me.wsSend(msg.Payload)
			if err != nil {
				// use goroutine to fix blocking: SwitchState (wait onNormal - onNormal wait SwitchState)
				go func() {
					me.stopSend <- true
					me.SwitchState(CLOSED)
				}()

				continue
			}

			log.Printf("[wsworker: %s] sent: %s", me.id, string(msg.Payload))

		case <-me.stopSend:
			break loop
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

	if me.state == NORMAL {
		me.stopSend <- true
		me.stopChopReplayQueue <- true
	}
}

func (me *Worker) onDead() {
	log.Printf("[wsworker: %s] onDead", me.id)

	if me.ws != nil {
		me.ws.Close()
	}

	if me.state == NORMAL {
		me.stopSend <- true
		me.stopChopReplayQueue <- true
	}

	// use goroutine to fix blocking: deadChecker (wait onDead - onDead wait deadChecker)
	go func() {
		me.stopDeadChecker <- true
	}()

	go func() {
		me.deadChan <- me.id
	}()
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

// remove committed messages from replay queue
func (me *Worker) chopReplayQueue() {
	log.Printf("[wsworker: %s] chopReplayQueue", me.id)

	ticker := time.NewTicker(1 * time.Second)
loop:
	for {
		select {
		case <-ticker.C:
			log.Printf("[wsworker: %s] chopReplayQueue choping", me.id)

			if me.state != NORMAL {
				return
			}

			go func() {
				me.commitChan <- Commit{
					Id:     me.id,
					Offset: me.commitOffset,
				}
			}()

			newReplayQueue := []*Message{}

			for _, msg := range me.replayQueue {
				if msg.Offset > me.commitOffset {
					newReplayQueue = append(newReplayQueue, msg)
				}
			}

			me.replayQueue = newReplayQueue
		case <-me.stopChopReplayQueue:
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
loop:
	for {
		_, p, err := ws.ReadMessage()
		if err != nil {
			log.Printf("[wsworker: %s] read message error: %s", me.id, err)
			me.SwitchState(CLOSED)
			break loop
		}

		offset := strToInt(string(p))

		if offset > me.commitOffset {
			me.commitOffset = offset
		}

		me.lastCommitTime = time.Now()
	}
}

func (me *Worker) debug() {
	log.Println("-------------------------------------------------")
	log.Printf("[wsworker: %s] state: %s\n", me.id, me.state)
	log.Printf("[wsworker: %s] replayQueue: %v\n", me.id, me.replayQueue)
	log.Printf("[wsworker: %s] commitOffset: %d\n", me.id, me.commitOffset)
	log.Printf("[wsworker: %s] lastCloseTime: %s\n", me.id, me.lastCloseTime)
	log.Printf("[wsworker: %s] lastCommitTime: %s\n", me.id, me.lastCommitTime)
}

func strToInt(str string) int {
	i, _ := strconv.Atoi(str)
	return i
}

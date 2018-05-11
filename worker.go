package wsworker

import (
	"github.com/gorilla/websocket"
	"log"
	"net/http"
	"strconv"
	"time"
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
	id          Id
	chopChan    chan int
	msgChan     <-chan Message
	deadChan    chan<- Id
	commitChan  chan<- Commit
	config      *Config
	ws          *websocket.Conn
	state       string
	replayQueue []*Message
	stateChan   chan State
	newConnChan chan bool
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
		chopChan:    make(chan int),
		state:       CLOSED,
		stateChan:   make(chan State),
		newConnChan: make(chan bool),
	}

	go w.stateSwitcher()
	return w
}

func (me *Worker) SetConnection(r *http.Request, w http.ResponseWriter) error {
	log.Printf("[wsworker: %s] SetConnection", me.id)

	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return err
	}
	me.ws = ws
	me.newConnChan <- true
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
	committed, offset := time.Now(), -1
	ws, currentoffset, closechan := me.ws, 0, make(chan bool)
	go func() {
		for {
			_, p, err := ws.ReadMessage()
			if err != nil {
				log.Printf("[wsworker: %s] read message error: %s", me.id, err)
				closechan <- true
			}

			offset := strToInt(string(p))
			if offset > currentoffset {
				me.chopChan <- offset
				currentoffset = offset
			}
		}
	}()

	for {
		select {
		case <-me.newConnChan:
			go me.SwitchState(NORMAL)
			return
		case <-closechan:
			go me.SwitchState(CLOSED)
			return
		case offset = <-me.chopChan:
		case <-time.After(1 * time.Second):
			// dead if haven't committed for too long
			if me.config.commitTimeout < time.Since(committed) {
				go me.SwitchState(DEAD)
				return
			}

			// check for commit offset
			if offset == -1 {
				continue
			}

			me.commitChan <- Commit{Id: me.id, Offset: offset}
			me.replayQueue = chop(me.replayQueue, offset)
			offset = -1
			committed = time.Now()

		case msg := <-me.msgChan:
			me.replayQueue = append(me.replayQueue, &msg)
			if err := me.wsSend(ws, msg.Payload); err != nil {
				go me.SwitchState(CLOSED)
				return
			}
		}
	}
}

func (me *Worker) onReplay() {
	log.Printf("[wsworker: %s] onReplay", me.id)
	log.Printf("[wsworker: %s] total message in queue: %d", me.id, len(me.replayQueue))
	ws := me.ws
	for _, m := range me.replayQueue {
		if err := me.wsSend(ws, m.Payload); err != nil {
			go me.SwitchState(CLOSED)
			return
		}

		log.Printf("[wsworker: %s] replayed: %s", me.id, string(m.Payload))
	}
	me.SwitchState(NORMAL)
}

func (me *Worker) onClosed() {
	log.Printf("[wsworker: %s] onClosed", me.id)

	for {
		select {
		case <-time.After(me.config.closeTimeout):
			go me.SwitchState(DEAD)
			return
		case <-me.newConnChan:
			go me.SwitchState(REPLAY)
			return
		}
	}
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

func (me *Worker) TotalInQueue() int {
	return len(me.replayQueue)
}

func (me *Worker) GetState() string {
	return me.state
}

func (me *Worker) SetConfig(config Config) {
	me.config = &config
}

func (me *Worker) wsSend(ws *websocket.Conn, payload []byte) error {
	if ws == nil {
		return nil
	}
	return ws.WriteMessage(websocket.TextMessage, payload)
}

func (me *Worker) debug() {
	log.Println("-------------------------------------------------")
	log.Printf("[wsworker: %s] state: %s\n", me.id, me.state)
	log.Printf("[wsworker: %s] replayQueue: %v\n", me.id, me.replayQueue)
}

func strToInt(str string) int {
	i, _ := strconv.Atoi(str)
	return i
}

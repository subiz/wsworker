package wsworker

import (
	"bitbucket.org/subiz/fsm"
	"github.com/gorilla/websocket"
	"log"
	"net/http"
	"strconv"
	"time"
)

const (
	CLOSED  = "closed"
	DEAD    = "dead"
	NORMAL  = "normal"
	REPLAY  = "replay"
	ECLOSED = "e_closed"
	EDEAD   = "e_dead"
	ENORMAL = "e_normal"
	EREPLAY = "e_replay"
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
	newConnChan chan bool
	machine     fsm.FSM
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
		newConnChan: make(chan bool),
		machine:     fsm.New(id),
	}

	w.machine.Map([]fsm.State{
		{ECLOSED, CLOSED, w.OnClosed},
		{EDEAD, DEAD, w.OnDead},
		{ENORMAL, NORMAL, w.OnNormal},
		{EREPLAY, REPLAY, w.OnReplay},
	})
	w.machine.Run(ECLOSED, nil)
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

func chop(queue []*Message, offset int) []*Message {
	for i, msg := range queue {
		if offset < msg.Offset {
			return queue[i:]
		}
	}
	return nil
}

func (me *Worker) OnNormal(_ string, _ interface{}) (string, interface{}) {
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
			return ENORMAL, nil
		case <-closechan:
			return ECLOSED, nil
		case offset = <-me.chopChan:
		case <-time.After(1 * time.Second):
			// dead if haven't committed for too long
			if me.config.commitTimeout < time.Since(committed) {
				return EDEAD, nil
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
				log.Printf("[wsworker: %s] on send error %v", me.id, err)
				return ECLOSED, nil
			}
		}
	}
}

func (me *Worker) OnReplay(_ string, _ interface{}) (string, interface{}) {
	log.Printf("[wsworker: %s] onReplay", me.id)
	log.Printf("[wsworker: %s] total message in queue: %d", me.id, len(me.replayQueue))
	ws := me.ws
	for _, m := range me.replayQueue {
		if err := me.wsSend(ws, m.Payload); err != nil {
			log.Printf("[wsworker: %s] on send error %v", me.id, err)
			return ECLOSED, nil
		}

		log.Printf("[wsworker: %s] replayed: %s", me.id, string(m.Payload))
	}
	return ENORMAL, nil
}

func (me *Worker) OnClosed(_ string, _ interface{}) (string, interface{}) {
	log.Printf("[wsworker: %s] onClosed", me.id)

	for {
		select {
		case <-time.After(me.config.closeTimeout):
			return DEAD, nil
		case <-me.newConnChan:
			return REPLAY, nil
		}
	}
}

func (me *Worker) OnDead(_ string, _ interface{}) (string, interface{}) {
	log.Printf("[wsworker: %s] onDead", me.id)

	// release resource
	if me.ws != nil {
		me.ws.Close()
	}
	me.ws = nil
	me.replayQueue = nil

	me.deadChan <- me.id
	me.machine.Stop()
	return "", nil
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

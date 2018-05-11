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
	replayQueue []*Message
	newConnChan chan *websocket.Conn
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
		newConnChan: make(chan *websocket.Conn),
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
	me.newConnChan <- ws
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

func tryRead(c chan bool) bool {
	select{
	case c := <- c:
		return c
	default:
	}
	return false
}

func (me *Worker) OnNormal(_ string, wsi interface{}) (string, interface{}) {
	log.Printf("[wsworker: %s] onNormal", me.id)
	committed, offset := time.Now(), -1
	ws := wsi.(*websocket.Conn)
	currentoffset, closechan := 0, make(chan bool, 2)
	go func() {
		for {
			_, p, err := ws.ReadMessage()
			if err != nil {
				log.Printf("[wsworker: %s] read message error: %s", me.id, err)
				closechan <- true
				return
			}
			offset := strToInt(string(p))
			if offset > currentoffset {
				me.chopChan <- offset
				currentoffset = offset
			}
		}
	}()

	defer ws.Close()
	for {
		select {
		case newws := <-me.newConnChan:
			return ENORMAL, newws
		case <-closechan:
			return ECLOSED, ws
		case offset = <-me.chopChan:
		case <-time.After(1 * time.Second):
			// dead if haven't committed for too long
			if me.config.commitTimeout < time.Since(committed) {
				return EDEAD, ws
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
				return ECLOSED, ws
			}
		}
	}
}

func (me *Worker) OnReplay(_ string, wsi interface{}) (string, interface{}) {
	log.Printf("[wsworker: %s] onReplay", me.id)
	log.Printf("[wsworker: %s] total message in queue: %d", me.id, len(me.replayQueue))
	ws := wsi.(*websocket.Conn)
	for _, m := range me.replayQueue {
		if err := me.wsSend(ws, m.Payload); err != nil {
			log.Printf("[wsworker: %s] on send error %v", me.id, err)
			return ECLOSED, ws
		}

		log.Printf("[wsworker: %s] replayed: %s", me.id, string(m.Payload))
	}
	return ENORMAL, ws
}

func (me *Worker) OnClosed(_ string, wsi interface{}) (string, interface{}) {
	log.Printf("[wsworker: %s] onClosed", me.id)
	ws := wsi.(*websocket.Conn)
	for {
		select {
		case <-time.After(me.config.closeTimeout):
			return EDEAD, ws
		case ws := <-me.newConnChan:
			return EREPLAY, ws
		}
	}
}

func (me *Worker) OnDead(_ string, wsi interface{}) (string, interface{}) {
	log.Printf("[wsworker: %s] onDead", me.id)
	// release resource
	ws := wsi.(*websocket.Conn)
	ws.Close()

	me.replayQueue = nil
	me.deadChan <- me.id
	me.machine.Stop()
	return "", nil
}

func (me *Worker) TotalInQueue() int {
	return len(me.replayQueue)
}

func (me *Worker) GetState() string {
	return me.machine.GetState()
}

func (me *Worker) SetConfig(config Config) {
	me.config = &config
}

func (me *Worker) wsSend(ws *websocket.Conn, payload []byte) error {
	return ws.WriteMessage(websocket.TextMessage, payload)
}

func (me *Worker) debug() {
	log.Println("-------------------------------------------------")
	log.Printf("[wsworker: %s] state: %s\n", me.id, me.machine.GetState())
	log.Printf("[wsworker: %s] replayQueue: %v\n", me.id, me.replayQueue)
}

func strToInt(str string) int {
	i, _ := strconv.Atoi(str)
	return i
}

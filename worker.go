package wsworker

import (
	"bitbucket.org/subiz/fsm"
	"bitbucket.org/subiz/wsworker/driver/gorilla"
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

type Ws interface {
	Close() error
	Recv() <-chan []byte
	RecvErr() <-chan error
	Ping() error
	Send(data []byte) error
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
	pingTimeout   time.Duration
	closeTimeout  time.Duration
	commitTimeout time.Duration
}

type Worker struct {
	id          Id
	msgChan     <-chan Message
	deadChan    chan<- Id
	commitChan  chan<- Commit
	config      *Config
	replayQueue []*Message
	newConnChan chan Ws
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
			pingTimeout:   30 * time.Minute,
			closeTimeout:  5 * time.Minute,
			commitTimeout: 30 * time.Minute,
		},
		newConnChan: make(chan Ws),
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

	ws, err := gorilla.NewWs(w, r, nil)
	if err != nil {
		return err
	}
	me.newConnChan <- ws
	return nil
}

// chop queue, return new queue and the first offset
func chop(queue []*Message, offset int) ([]*Message, bool) {
	if len(queue) == 0 {
		return queue, false
	}

	if offset < queue[0].Offset {
		return queue, false
	}

	for i, msg := range queue {
		if offset < msg.Offset {
			return queue[i:], true
		}
	}
	return nil, true
}

func (me *Worker) OnNormal(_ string, wsi interface{}) (string, interface{}) {
	log.Printf("[wsworker: %s] onNormal", me.id)
	ws, committed, pinged, offset := wsi.(Ws), time.Now(), time.Now(), -1
	recv, recverr := ws.Recv(), ws.RecvErr()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if me.config.pingTimeout < time.Since(pinged) {
				if err := ws.Ping(); err != nil {
					return ECLOSED, ws
				}
				pinged = time.Now()
			}

			// dead if haven't committed for too long
			if me.config.commitTimeout < time.Since(committed) && 0 < len(me.replayQueue) {
				return EDEAD, ws
			}

			// check for commit offset
			if offset < 0 {
				continue
			}

			newqueue, ok := chop(me.replayQueue, offset)
			if ok {
				me.commitChan <- Commit{Id: me.id, Offset: offset}
			}

			me.replayQueue, offset, committed = newqueue, -1, time.Now()
		case err := <-recverr:
			log.Printf("[wsworker: %s] recv error: %s", me.id, err)
			return ECLOSED, ws
		case p := <-recv: // inconmming message from ws
			offset = strToInt(string(p))
		case newws := <-me.newConnChan:
			ws.Close()
			return ENORMAL, newws
		case msg := <-me.msgChan:
			me.replayQueue = append(me.replayQueue, &msg)
			if err := ws.Send(msg.Payload); err != nil {
				log.Printf("[wsworker: %s] on send error %v", me.id, err)
				return ECLOSED, ws
			}
		}
	}
}

func (me *Worker) OnReplay(_ string, wsi interface{}) (string, interface{}) {
	log.Printf("[wsworker: %s] onReplay", me.id)
	log.Printf("[wsworker: %s] total message in queue: %d", me.id, len(me.replayQueue))
	ws := wsi.(Ws)
	for _, m := range me.replayQueue {
		if err := ws.Send(m.Payload); err != nil {
			log.Printf("[wsworker: %s] on send error %v", me.id, err)
			return ECLOSED, ws
		}
		log.Printf("[wsworker: %s] replayed: %s", me.id, string(m.Payload))
	}
	return ENORMAL, ws
}

func (me *Worker) OnClosed(_ string, wsi interface{}) (string, interface{}) {
	log.Printf("[wsworker: %s] onClosed", me.id)
	ws := wsi.(Ws)
	for {
		select {
		case <-time.After(me.config.closeTimeout):
			return EDEAD, ws
		case newws := <-me.newConnChan:
			ws.Close()
			return EREPLAY, newws
		}
	}
}

func (me *Worker) OnDead(_ string, wsi interface{}) (string, interface{}) {
	log.Printf("[wsworker: %s] onDead", me.id)
	// release resource
	ws := wsi.(Ws)
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

func (me *Worker) debug() {
	log.Println("-------------------------------------------------")
	log.Printf("[wsworker: %s] state: %s\n", me.id, me.machine.GetState())
	log.Printf("[wsworker: %s] replayQueue: %v\n", me.id, me.replayQueue)
}

func strToInt(str string) int {
	i, _ := strconv.Atoi(str)
	return i
}

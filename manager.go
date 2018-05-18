package wsworker

import (
	"time"
	"sync"
	"bitbucket.org/subiz/wsworker/driver/gorilla"
	"net/http"
)

type Mgr struct {
	deadDeadline time.Duration // 2 min

	onReplayC chan *Client
	onDeadC chan *Client
	onNormalC chan *Client
	onClosedC chan *Client

	deadClientM map[string]*Client
	normalClientM map[string]*Client
	replayClientM map[string]*Client
	closedClientM map[string]*Client

	closedNewConnC chan *Conn
	normalNewConnC chan *Conn

	closedLock *sync.Mutex
	normalLock *sync.Mutex
	replayLock *sync.Mutex
	deadLock *sync.Mutex

}



func getClient(lock *sync.Mutex, clientM map[string]*Client, id string) (*Client, bool){
	lock.Lock()
	c, ok := clientM[id]
	lock.Unlock()
	return c, ok
}

func removeClient(lock *sync.Mutex, clientM map[string]*Client, id string) {
	lock.Lock()
	delete(clientM, id)
	lock.Unlock()
}

func addClient(lock *sync.Mutex, clientM map[string]*Client, id string, client *Client) {
	lock.Lock()
	clientM[id] = client
	lock.Unlock()
}

func loopClient(lock *sync.Mutex, clientM map[string]*Client, f func(string, *Client) bool) {
	lock.Lock()
	for id, c := range clientM {
		lock.Unlock()
		if !f(id, c) {
			return
		}
		lock.Lock()
	}
	lock.Unlock()
}

type Client struct {
	id string
	offset int
	replayQueue []*Message
	ws Ws
	closed time.Time
	state string
	lock *sync.Mutex
	dirty bool
	listening bool
}

func (c *Client) Relisten(ws Ws) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.listening = false

	if c.ws != nil {
		if err := c.ws.Close(); err != nil {
			return err
		}
	}
	c.ws = ws
	go c.Listen()
	return nil
}

func (c *Client) Listen() {
	c.lock.Lock()
	for c.listening = true; c.listening; {
		c.lock.Unlock()
		p := <- c.ws.Recv()
		newoff := strToInt(string(p))
		c.lock.Lock()
		if newoff > c.offset {
			c.offset = newoff
			c.dirty = true
		}
	}
	c.lock.Unlock()
}

func (m *Mgr) newClient(id string) *Client {
	client := &Client{
		id: id,
		offset: -1,
		closed: time.Now(),
		state: NORMAL,
		lock: &sync.Mutex{},
		listening: true,
	}
	go func() {

	}()

	return client
}

type Conn struct {
	id string
	ws Ws
}

func (m *Mgr) findClient(id string) *Client {
	if c, ok := getClient(m.closedLock, m.closedClientM, id); ok {
		return c
	}

	if c, ok := getClient(m.nornamLock, m.normalClientM, id); ok {
		return c
	}

	if c, ok := getClient(m.deadLock, m.deadClientM, id); ok {
		return c
	}

	if c, ok := getClient(m.replayLock, m.replayClientM, id); ok {
		return c
	}
	return nil
}

func (m *Mgr) Reconnect(r *http.Request, w http.ResponseWriter, id string) error {
	ws, err := gorilla.NewWs(w, r, nil)
	if err != nil {
		return err
	}

	client := m.findClient(id)
	if client == nil {
		ws.Close()
		m.deadChan <- id
	}

	client.lock.Lock()
	state := client.state
	client.lock.Unlock()

	switch state {
	case NORMAL:
		m.normalNewConnC <- &Conn{id: id, ws: ws}
	case CLOSED:
		m.closedNewConnC <- &Conn{id: id, ws: ws}
	case DEAD, REPLAY:
		ws.Close()
	default:
		ws.Close()
	}
	return nil
}

func (m *Mgr) NewConnection(r *http.Request, w http.ResponseWriter, id string) error {
	ws, err := gorilla.NewWs(w, r, nil)
	if err != nil {
		return err
	}

	client := m.findClient(id)
	if client != nil {
		ws.Close()
		return nil
	}

	m.normalNewConnC <- &Conn{id: id, ws: ws}
	return nil
}

func (m *Mgr) OnDead(clientM map[string]*Client) {
	for client := range m.onDeadC {
		client.lock.Lock()
		client.state = DEAD
		client.lock.Unlock()
		m.deadChan <- client.id
	}
}

func (m *Mgr) onNormal(clientM map[string]*Client) {
	pingTicker := time.NewTicker(m.pingDeadline)
	dieTicker := time.NewTicker(m.commitDeadline)
	commitTicker := time.NewTicker(1 * time.Second)
	select {
	case <- pingTicker.C:
		loopClient(m.normalLock, clientM, func(id string, client *Client) bool {
			if time.Since(client.closed) < m.pingDeadline {
				return true
			}

			if err := client.ws.Ping(); err != nil {
				fmt.Print(err)
				client.lock.Lock()
				removeClient(m.normalLock, clientM, id)
				m.onClosedC <- client
				client.lock.Unlock()
				return true
			}
			return true
		})
	case <- dieTicker.C:
		loopClient(m.normalLock, clientM, func(id string, client *Client) bool {
			if time.Since(client.closed) < m.commitDeadline ||
				len(client.replayQueue) == 0 {
				return true
			}
			client.lock.Lock()
			removeClient(m.normalLock, clientM, id)
			m.onDeadC <- client
			client.lock.Unlock()
			return true
		})

	case <- commitTicker.C:
		loopClient(m.normalLock, clientM, func(id string, client *Client) bool {
			if client.err != nil {
				log.Printf("[wsworker: %s] recv error: %s", me.id, err)
				client.lock.Lock()
				removeClient(m.normalLock, clientM, id)
				m.onClosedC <- client
				client.lock.Unlock()
				return true
			}

			if !client.dirty {
				return true
			}
			client.lock.Lock()
			q, ok = chop(client.replayQueue, client.offset)
			if ok {
				m.commitChan <- Commit{Id: me.id, Offset: client.offset}
			}
			client.replayQueue, client.dirty = q, false
			client.lock.Unlock()
		})
	case conn := <- m.normalNewConnC:
		client, ok := getClient(m.normalLock, clientM, conn.id)
		if !ok {
			client = newClient(conn.id)
			client.Relisten(conn.ws)
		}

		client.lock.Lock()
		addClient(m.normalLock, clientM, msg.id, client)
		client.lock.Unlock()

	case msg := <- m.normalMsgC:
		client, ok := getClient(m.normalLock, clientM, msg.id)
		if !ok {
			m.normalMsgC <- nil
			panic("should be ok")
		}
		client.replayQueue = append(client.replayQueue, &msg.message)
		err := client.ws.Send(msg.Payload)
		m.normalMsgC <- nil
		if err == nil {
			return
		}
		log.Printf("[wsworker: %s] on send error %v", me.id, err)

		client.lock.Lock()
		removeClient(m.normalLock, clientM, id)
		m.onClosedC <- client
		client.lock.Unlock()
	}
	return nil
}

func (m *Mgr) OnClosed() {
	for {
		m.onClosed(m.closedClientM)
	}
}

func (m *Mgr) onClosed(clientM map[string]*Client) {
	deadTicker := time.NewTicker(m.deadDeadline)
	clientM := make(map[int64]*Client, 1000000)
	select {
	case conn := <- m.closedNewConnC:
		c, ok := getClient(m.closeLock, clientM, conn.id)
		if !ok {
			conn.ws.Close()
			break
		}
		c.lock.Lock()
		removeClient(m.closedLock, clientM, c.id)
		c.Relisten(conn.ws)
		c.lock.Unlock()
		m.onReplayC <- c
	case <- deadTicker.C:
		loopClient(m.closeLock, clientM, func(_ string, client *Client) bool {
			if time.Since(client.closed) < m.deadDeadline {
				return true
			}
			client.lock.Lock()
			removeClient(m.closeLock, clientM, client.id)
			m.onDeadC <- client
			client.lock.Unlock()
			return true
		})
	case client := <- m.onCloseC:
		client.lock.Lock()
		client.closed = time.Now()
		client.state = CLOSED
		client.lock.Unlock()
		addClient(m.closeLock, clientM, client.id, client)
	case msg := <- m.closeMsgC:
		client, ok := getClient(m.normalLock, clientM, msg.Id)
		if !ok {
			m.closeMsgC <- nil
			panic("should be ok")
		}
		client.replayQueue = append(client.replayQueue, &msg)
		m.closeMsgC <- nil
	}
}

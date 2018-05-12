package gorilla

import (
	"github.com/gorilla/websocket"
	"io"
	"net/http"
	"time"
)

type Ws struct {
	conn    *websocket.Conn
	writer  io.WriteCloser
	recv    chan []byte
	recverr chan error
	pingerr chan error
	stopped bool
}

func NewWs(w http.ResponseWriter, r *http.Request, h http.Header) (*Ws, error) {
	conn, err := upgrader.Upgrade(w, r, h)
	if err != nil {
		return nil, err
	}
	if err = conn.SetReadDeadline(time.Now().Add(PongWait)); err != nil {
		return nil, err
	}

	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(PongWait))
	})

	writer, err := conn.NextWriter(websocket.TextMessage)
	if err != nil {
		return nil, err
	}

	ws := &Ws{
		pingerr: make(chan error, 2),
		recv:    make(chan []byte, 2),
		recverr: make(chan error, 2),
		stopped: false,
		writer:  writer,
		conn:    conn,
	}

	go ws.pingLoop()
	go ws.readLoop()
	return ws, nil
}

var (
	// MaxMessageSize limit maximum message size allowed from peer
	MaxMessageSize = 4096

	// WriteWait is sending message timeout
	WriteWait = 21 * time.Second

	// PongWait is the time allowed to read the next pong message from the peer
	// PingPeriod is the period to send ping
	PongWait, PingPeriod = 26 * time.Second, PongWait * 9 / 10

	upgrader = websocket.Upgrader{
		ReadBufferSize: MaxMessageSize,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
		WriteBufferSize: MaxMessageSize,
	}
)

func (ws *Ws) Close() error {
	ws.stopped = true
	return ws.conn.Close()
}

func (ws *Ws) Recv() <-chan []byte {
	return ws.recv
}

func (ws *Ws) RecvErr() <-chan error {
	return ws.recverr
}

func (ws *Ws) PingErr() <-chan error {
	return ws.pingerr
}

func (ws *Ws) Send(data []byte) error {
	_, err := ws.writer.Write(data)
	return err
}

func (ws *Ws) pingLoop() {
	for ; !ws.stopped; time.Sleep(PingPeriod) {
		ws.conn.SetWriteDeadline(time.Now().Add(WriteWait))
		if err := ws.conn.WriteMessage(websocket.PingMessage, []byte{}); err != nil {
			ws.pingerr <- err
			ws.Close()
			return
		}
	}
}

func (ws *Ws) readLoop() {
	for !ws.stopped {
		_, p, err := ws.conn.ReadMessage()
		if err != nil {
			ws.recverr <- err
			ws.Close()
			return
		}
		ws.recv <- p
	}
}

package gorilla

import (
	"github.com/gorilla/websocket"
	"io"
	"net/http"
	"time"
)

type Ws struct {
	conn    *websocket.Conn
	recv    chan []byte
	recverr chan error
	stopped bool
}

func NewWs(w http.ResponseWriter, r *http.Request, h http.Header) (*Ws, error) {
	conn, err := upgrader.Upgrade(w, r, h)
	if err != nil {
		return nil, err
	}

	ws := &Ws{
		recv:    make(chan []byte, 2),
		recverr: make(chan error, 2),
		stopped: false,
		conn:    conn,
	}

	go ws.readLoop()
	return ws, nil
}

var (
	// MaxMessageSize limit maximum message size allowed from peer
	MaxMessageSize = 4096

	// WriteWait is sending message timeout
	WriteWait = 21 * time.Second

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

func (ws *Ws) Send(data []byte) error {
	writer, err := ws.conn.NextWriter(websocket.TextMessage)
	if err != nil {
		return err
	}
	if _, err := writer.Write(data); err != nil {
		return err
	}
	return writer.Close()
}

func (ws *Ws) Ping() error {
	ws.conn.SetWriteDeadline(time.Now().Add(WriteWait))
	if err := ws.conn.WriteMessage(websocket.PingMessage, []byte{}); err != nil {
		ws.Close()
		return err
	}
	return nil
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

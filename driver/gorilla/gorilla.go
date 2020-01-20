package gorilla

import (
	"fmt"
	"github.com/gorilla/websocket"
	"net/http"
	"sync"
	"time"
)

type Ws struct {
	mutex *sync.Mutex
	conn  *websocket.Conn
}

func NewWs(w http.ResponseWriter, r *http.Request, h http.Header) (*Ws, error) {
	conn, err := upgrader.Upgrade(w, r, h)
	if err != nil {
		return nil, err
	}
	return &Ws{mutex: &sync.Mutex{}, conn: conn}, nil
}

var (
	// MaxMessageSize limit maximum message size allowed from peer
	MaxMessageSize = 4096

	// WriteWait is sending message timeout
	WriteWait = 21 * time.Second

	upgrader = websocket.Upgrader{
		ReadBufferSize:  MaxMessageSize,
		CheckOrigin:     func(r *http.Request) bool { return true },
		WriteBufferSize: MaxMessageSize,
	}
)

func (ws *Ws) IsClosed() bool {
	return ws.conn != nil
}

func (ws *Ws) Close() error {
	if ws == nil {
		return nil
	}
	err := ws.conn.Close()
	ws.conn = nil
	return err
}

func (ws *Ws) Recv() ([]byte, error) {
	_, p, err := ws.conn.ReadMessage()
	if err != nil {
		ws.Close()
		return nil, err
	}
	return p, nil
}

func (ws *Ws) Send(data []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
			return
		}
	}()

	ws.mutex.Lock()
	defer ws.mutex.Unlock()

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
	if err := ws.conn.SetWriteDeadline(time.Now().Add(WriteWait)); err != nil {
		return err
	}
	if err := ws.conn.WriteMessage(websocket.PingMessage, []byte{}); err != nil {
		ws.Close()
		return err
	}
	return nil
}

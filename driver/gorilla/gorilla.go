package gorilla

import (
	"github.com/gorilla/websocket"
	"net/http"
	"time"
)

type Ws struct {
	conn *websocket.Conn
}

func NewWs(w http.ResponseWriter, r *http.Request, h http.Header) (*Ws, error) {
	conn, err := upgrader.Upgrade(w, r, h)
	if err != nil {
		return nil, err
	}
	ws := &Ws{conn: conn}
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
	return ws.conn.Close()
}

func (ws *Ws) Recv() ([]byte, error) {
	_, p, err := ws.conn.ReadMessage()
	if err != nil {
		ws.Close()
		return nil, err
	}
	return p, nil
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

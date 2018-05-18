package gobwas

import (
	//"github.com/mailru/easygo/netpoll"
	"github.com/gobwas/ws/wsutil"
	websocket "github.com/gobwas/ws"
	"io"
	"net"
	"net/http"
	_ "time"
)

type Ws struct {
	conn    net.Conn
	recv    chan []byte
	recverr chan error
	stopped bool
}

func NewWs(w http.ResponseWriter, r *http.Request, h http.Header) (*Ws, error) {
	conn, _, _, err := websocket.UpgradeHTTP(r, w, h)
	if err != nil {
		return nil, err
	}

	ws := &Ws{
		recv:    make(chan []byte, 20),
		recverr: make(chan error, 20),
		stopped: false,
		conn:    conn,
	}

	//poller, err := netpoll.New(nil)
	//if err != nil {
	//return nil, err
	//}

	// Get netpoll descriptor with EventRead|EventEdgeTriggered.
	//desc := netpoll.Must(netpoll.HandleRead(conn))

	//poller.Start(desc, func(ev netpoll.Event) {
	go func() {
		println("called")
		//if ev&netpoll.EventReadHup != 0 {
		//println("stop")
		//poller.Stop(desc)
		//conn.Close()
		//return
		//}

		for {
			msg, op, err := wsutil.ReadClientData(conn)
			if err != nil {
				println("eeeeeeeee", err.Error())
				return
				// handle error
			}

			println("op", op)


			/*
		tmp := make([]byte, 256)
		n, err := conn.Read(tmp)
		println("n", n)
		if err != nil {
			if err != io.EOF {
				fmt.Println("read error:", err)
			}
			return
		}
*/
			println("got data", string(msg))
			//ws.recv <- msg
		}
	}()

	return ws, nil
}

func (ws *Ws) Close() error {
	return ws.conn.Close()
}

func (ws *Ws) Recv() <-chan []byte {
	return ws.recv
}

func (ws *Ws) RecvErr() <-chan error {
	return ws.recverr
}

func (ws *Ws) Send(data []byte) error {
	header, err := websocket.ReadHeader(ws.conn)
	if err != nil {
		return err
	}

	if err := websocket.WriteHeader(ws.conn, header); err != nil {
		return err
	}
	if _, err := ws.conn.Write(data); err != nil {
		return err
	}

	if header.OpCode == websocket.OpClose {
		return ws.Close()
	}
	return nil
}

func (ws *Ws) Ping() error {
	return nil
}

func (ws *Ws) readLoop() {
	defer ws.conn.Close()
	for {
		println("hihi")
		header, err := websocket.ReadHeader(ws.conn)
		if err != nil {
			println("eeee")
			ws.recverr <- err
			break
		}
		println("hihixxxxxxx", header.Length)

		payload := make([]byte, 0, header.Length)
		if n, err := io.ReadFull(ws.conn, payload); err != nil {
			println("what", err.Error())
			ws.recverr <- err
			break
		} else {
			println("nnnnnnn", n)
		}
		println("got", string(payload))
		ws.recv <- payload
		if header.Masked {
			websocket.Cipher(payload, header.Mask, 0)
		}

		// Reset the Masked flag, server frames must not be masked as
		// RFC6455 says.
		header.Masked = false

		if header.OpCode == websocket.OpClose {
			return
		}
	}
}

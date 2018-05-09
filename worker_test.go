package wsworker

import (
	"bytes"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestSendMessage(t *testing.T) {
	expected := []byte("{\"id\":1}")

	connId := "1"
	msgChan := make(chan Message, 100)

	msgChan <- Message{
		Id:      connId,
		Offset:  1,
		Payload: expected,
	}

	worker := NewWorker(connId, msgChan, make(chan Id), make(chan Commit))

	http.HandleFunc("/echo1", func(w http.ResponseWriter, r *http.Request) {
		worker.SetConnection(r, w)
	})

	addr := ":9001"

	go func() {
		log.Fatal(http.ListenAndServe(addr, nil))
	}()

	time.Sleep(50 * time.Millisecond)

	u := url.URL{Scheme: "ws", Host: addr, Path: "/echo1"}
	c, _, _ := websocket.DefaultDialer.Dial(u.String(), nil)
	defer c.Close()

	_, got, _ := c.ReadMessage()

	if !bytes.Equal(got, expected) {
		t.Errorf("expected %s, got: %s", expected, got)
	}
}

func TestReplayMessage(t *testing.T) {
	expected := []byte("{\"id\":1}")
	connId := "2"
	msgChan := make(chan Message, 100)

	msgChan <- Message{
		Id:      connId,
		Offset:  1,
		Payload: expected,
	}

	worker := NewWorker(connId, msgChan, make(chan Id), make(chan Commit))

	http.HandleFunc("/echo2", func(w http.ResponseWriter, r *http.Request) {
		worker.SetConnection(r, w)
	})

	addr := ":9002"

	go func() {
		log.Fatal(http.ListenAndServe(addr, nil))
	}()

	time.Sleep(50 * time.Millisecond)

	u := url.URL{Scheme: "ws", Host: addr, Path: "/echo2"}

	c1, _, _ := websocket.DefaultDialer.Dial(u.String(), nil)
	c1.Close()

	c2, _, _ := websocket.DefaultDialer.Dial(u.String(), nil)
	defer c2.Close()
	_, got, _ := c2.ReadMessage()

	if !bytes.Equal(got, expected) {
		t.Errorf("expected %s, got: %s", expected, got)
	}
}

func TestChopReplayQueue(t *testing.T) {
	connId := "3"
	msgChan := make(chan Message, 100)

	total := 2

	for i := 1; i <= total; i++ {
		msgChan <- Message{
			Id:      connId,
			Offset:  i,
			Payload: []byte("{\"id\":" + strconv.Itoa(i) + "}"),
		}
	}

	worker := NewWorker(connId, msgChan, make(chan Id), make(chan Commit))

	http.HandleFunc("/echo3", func(w http.ResponseWriter, r *http.Request) {
		worker.SetConnection(r, w)
	})

	addr := ":9003"

	go func() {
		log.Fatal(http.ListenAndServe(addr, nil))
	}()

	time.Sleep(50 * time.Millisecond)

	u := url.URL{Scheme: "ws", Host: addr, Path: "/echo3"}
	c, _, _ := websocket.DefaultDialer.Dial(u.String(), nil)
	defer c.Close()

	c.WriteMessage(websocket.TextMessage, []byte(strconv.Itoa(total)))

	time.Sleep(2 * time.Second)

	expected := 0
	got := worker.TotalInQueue()
	if got != expected {
		t.Errorf("expected %d, got: %d", expected, got)
	}
}

func TestCloseTimeout(t *testing.T) {
	worker := NewWorker("4", make(chan Message), make(chan Id), make(chan Commit))

	closeTimeout := 2 * time.Second
	worker.SetConfig(Config{
		closeTimeout:  closeTimeout,
		commitTimeout: 30 * time.Minute,
	})

	http.HandleFunc("/echo4", func(w http.ResponseWriter, r *http.Request) {
		worker.SetConnection(r, w)
	})

	addr := ":9004"

	go func() {
		log.Fatal(http.ListenAndServe(addr, nil))
	}()

	time.Sleep(50 * time.Millisecond)

	u := url.URL{Scheme: "ws", Host: addr, Path: "/echo4"}

	c, _, _ := websocket.DefaultDialer.Dial(u.String(), nil)
	c.Close()

	checkerInterval := 1 * time.Second
	time.Sleep(closeTimeout + checkerInterval)

	expected := DEAD
	got := worker.GetState()

	if got != expected {
		t.Errorf("expected %s, got: %s", expected, got)
	}
}

func TestCommitTimeout(t *testing.T) {
	worker := NewWorker("5", make(chan Message), make(chan Id), make(chan Commit))

	commitTimeout := 2 * time.Second
	worker.SetConfig(Config{
		closeTimeout:  5 * time.Minute,
		commitTimeout: commitTimeout,
	})

	http.HandleFunc("/echo5", func(w http.ResponseWriter, r *http.Request) {
		worker.SetConnection(r, w)
	})

	addr := ":9005"

	go func() {
		log.Fatal(http.ListenAndServe(addr, nil))
	}()

	time.Sleep(50 * time.Millisecond)

	u := url.URL{Scheme: "ws", Host: addr, Path: "/echo5"}

	c, _, _ := websocket.DefaultDialer.Dial(u.String(), nil)
	defer c.Close()

	checkerInterval := 1 * time.Second
	time.Sleep(commitTimeout + checkerInterval)

	expected := DEAD
	got := worker.GetState()

	if got != expected {
		t.Errorf("expected %s, got: %s", expected, got)
	}
}

func TestNormalState(t *testing.T) {
	worker := NewWorker("6", make(chan Message), make(chan Id), make(chan Commit))
	expected := NORMAL

	worker.SwitchState(REPLAY)
	worker.SwitchState(expected)

	got := worker.GetState()

	if got != expected {
		t.Errorf("expected %s, got: %s", expected, got)
	}
}

func TestReplayState(t *testing.T) {
	worker := NewWorker("7", make(chan Message), make(chan Id), make(chan Commit))

	expected := REPLAY
	worker.SwitchState(expected)
	got := worker.GetState()

	if got != expected {
		t.Errorf("expected %s, got: %s", expected, got)
	}
}

func TestClosedState(t *testing.T) {
	worker := NewWorker("8", make(chan Message), make(chan Id), make(chan Commit))

	expected := CLOSED
	worker.SwitchState(expected)
	got := worker.GetState()

	if got != expected {
		t.Errorf("expected %s, got: %s", expected, got)
	}
}

func TestDeadState(t *testing.T) {
	worker := NewWorker("9", make(chan Message), make(chan Id), make(chan Commit))

	expected := DEAD
	worker.SwitchState(expected)
	got := worker.GetState()

	if got != expected {
		t.Errorf("expected %s, got: %s", expected, got)
	}
}

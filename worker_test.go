package wsworker

import (
	"bytes"
	// "encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestSend(t *testing.T) {
	expected := []byte("{\"id\":1}")

	connId := "1"
	msgChan := make(chan Message, 100)
	deadChan := make(chan Id)
	commitChan := make(chan Commit)

	worker := NewWorker(connId, msgChan, deadChan, commitChan)

	msgChan <- Message{
		Id:      connId,
		Offset:  1,
		Payload: expected,
	}

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

	_, got, _ := c.ReadMessage()

	if !bytes.Equal(got, expected) {
		t.Errorf("expected %s, got: %s", expected, got)
	}
}

func TestReplay(t *testing.T) {
	expected := []byte("{\"id\":1}")

	connId := "2"
	msgChan := make(chan Message, 100)
	deadChan := make(chan Id)
	commitChan := make(chan Commit)

	worker := NewWorker(connId, msgChan, deadChan, commitChan)

	http.HandleFunc("/echo2", func(w http.ResponseWriter, r *http.Request) {
		err := worker.SetConnection(r, w)
		if err != nil {
			fmt.Printf("set connection error: %s\n", err)
		}
	})

	addr := ":9002"

	go func() {
		log.Fatal(http.ListenAndServe(addr, nil))
	}()

	time.Sleep(50 * time.Millisecond)

	u := url.URL{Scheme: "ws", Host: addr, Path: "/echo2"}

	c1, _, _ := websocket.DefaultDialer.Dial(u.String(), nil)
	c1.Close()

	time.Sleep(50 * time.Millisecond)

	msgChan <- Message{
		Id:      connId,
		Offset:  1,
		Payload: expected,
	}

	c2, _, _ := websocket.DefaultDialer.Dial(u.String(), nil)
	_, got, _ := c2.ReadMessage()

	if !bytes.Equal(got, expected) {
		t.Errorf("expected %s, got: %s", expected, got)
	}
}

func TestChopReplayQueue(t *testing.T) {
	message := []byte("{\"id\":1}")

	connId := "3"
	msgChan := make(chan Message, 100)
	deadChan := make(chan Id)
	commitChan := make(chan Commit)

	worker := NewWorker(connId, msgChan, deadChan, commitChan)

	for i := 1; i <= 2; i++ {
		msgChan <- Message{
			Id:      connId,
			Offset:  i,
			Payload: message,
		}
	}

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

	_, msg, _ := c.ReadMessage()

	if bytes.Equal(msg, message) {
		c.WriteMessage(websocket.TextMessage, []byte("2"))
	}

	time.Sleep(2 * time.Second)

	expected := 0
	got := worker.TotalInQueue()
	if got != expected {
		t.Errorf("expected %d, got: %d", expected, got)
	}
}

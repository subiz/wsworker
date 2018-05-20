package wsworker

import (
	"testing"
	"golang.org/x/net/websocket"
	"log"
	"fmt"
	"net/http"
	"time"
)
// create 100 client
// connected with difference id
// now send message to those id
// clients should receive all of those message and send commit
// commit chan should receive those commit

func TestManage(t *testing.T) {
	msgChan := make(chan *Message, 1000)
	deadChan:= make(chan string, 1000)
	commitChan:= make(chan Commit, 1000)
	mgr := NewManager(msgChan, deadChan, commitChan)


	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		cid := r.URL.Query().Get("connection_id")
		if err := mgr.SetConnection(r, w, cid); err != nil {
			t.Fatalf("err %v", err)
		}
	})

	go func() {
		log.Fatal(http.ListenAndServe(":8080", nil))
	}()

	time.Sleep(100 * time.Millisecond)
	for i := 0; i < 10; i++ {
		go doClient(toWsId(i), "ws://localhost:8080", "http://localhost/")
	}

	time.Sleep(100 * time.Millisecond)

	for i := 0; i < 100; i++ {
		msgChan <- &Message{
			Id: toWsId(i % 10),
			Offset: int32(i),
			Payload: []byte(fmt.Sprintf("%d", i)),
		}
	}

	commits := make([]int32, 0)
	for c := range commitChan {
		if 89 < c.Offset {
			commits = append(commits, c.Offset)
			if len(commits) == 10 {
				break
			}
		}
	}
}

func toWsId(i int) string {
	return fmt.Sprintf("0WERASDF-%d", i)
}

func doClient(id string, url, origin string) {
	ws, err := websocket.Dial(url + "?connection_id="+id, "", origin)
	if err != nil {
    log.Fatalf("hix %v", err)
	}

	//if _, err := ws.Write([]byte("hello, world!\n")); err != nil {
    //log.Fatalf("hihi %v", err)
	//}
	for {
		msg := make([]byte, 512)
		var n int
		if n, err = ws.Read(msg); err != nil {
			//fmt.Println("hiahaha")
			log.Fatalf("this %v", err)
		}

		if _, err := ws.Write(msg[:n]); err != nil {
			log.Fatalf("hihi %v", err)
		}
	}
}

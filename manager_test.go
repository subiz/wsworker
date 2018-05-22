package wsworker

import (
	"fmt"
	"golang.org/x/net/websocket"
	"log"
	"net/http"
	"os"
	"runtime"
	"testing"
	"time"
)

func TestClosed(t *testing.T) {
	deadChan := make(chan string, 1000)
	commitChan := make(chan Commit, 1000)
	mgr = NewManager(deadChan, commitChan)

	go func() {
		time.Sleep(100 * time.Millisecond)
		for i := 0; i < 100; i++ {
			mgr.Send(toWsId(0), int64(i), []byte(fmt.Sprintf("%d", i)))
		}
	}()
	doClosedClient(toWsId(0), "ws://localhost:8081", "http://localhost/")

}

func handleHttp(mgr *Mgr) {
	log.Fatal(http.ListenAndServe(":8081", nil))
}

// create 100 client
// connected with difference id
// now send message to those id
// clients should receive all of those message and send commit
// commit chan should receive those commit

func TestMain(m *testing.M) {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		cid := r.URL.Query().Get("connection_id")
		if err := mgr.SetConnection(r, w, cid); err != nil {
			panic(err)
		}
	})
	go handleHttp(mgr)
	time.Sleep(100 * time.Millisecond)
	os.Exit(m.Run())
}

var mgr *Mgr

func TestNormal(t *testing.T) {
	t.Skip()
	deadChan := make(chan string, 1000)
	commitChan := make(chan Commit, 1000)
	mgr = NewManager(deadChan, commitChan)
	for i := 0; i < 10; i++ {
		go doClient(toWsId(i), "ws://localhost:8081", "http://localhost/")
	}

	time.Sleep(100 * time.Millisecond)

	for i := 0; i < 100; i++ {
		mgr.Send(toWsId(i%10), int64(i), []byte(fmt.Sprintf("%d", i)))
	}

	commits := make([]int64, 0)
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

func doClosedClient(id string, url, origin string) {
	ws, err := websocket.Dial(url+"?connection_id="+id, "", origin)
	if err != nil {
		log.Fatalf("hix %v", err)
	}

	//if _, err := ws.Write([]byte("hello, world!\n")); err != nil {
	//log.Fatalf("hihi %v", err)
	//}
	for i := 0; i < 4; i++ {
		msg := make([]byte, 512)
		var n int
		if n, err = ws.Read(msg); err != nil {
			log.Fatalf("this %v", err)
		}
		println("recev")

		if _, err := ws.Write(msg[:n]); err != nil {
			log.Fatalf("hihi %v", err)
		}
	}
	PrintMemUsage()
	time.Sleep(2 * time.Second)
	println("closing")
	//	time.Sleep(2 * time.Second)
	if err := ws.Close(); err != nil {
		panic(err)
	}
	time.Sleep(2 * time.Second)
	PrintMemUsage()
}

func doClient(id string, url, origin string) {
	ws, err := websocket.Dial(url+"?connection_id="+id, "", origin)
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

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc %v | Total %v KiB | Sys %v KiB | %v Go\n", bToKb(m.Alloc), bToKb(m.TotalAlloc), bToKb(m.Sys), runtime.NumGoroutine())
}

func bToKb(b uint64) uint64 {
	return b / 1024
}

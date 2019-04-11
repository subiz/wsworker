package wsworker

import (
	"fmt"
	"golang.org/x/net/websocket"
	"log"
	"net/http"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestClosed(t *testing.T) {
	deadChan := make(chan string, 1000)
	commitChan := make(chan int64, 1000)
	mgr = NewManager([]string{"localhost:6379"}, "", deadChan, commitChan)

	id := toWsId(0)

	go func() {
		time.Sleep(100 * time.Millisecond)
		for i := 0; i < 100; i++ {
			mgr.Send(id, int64(i), []byte(fmt.Sprintf("hello-%d", i)))
		}
	}()
	origin := "http://localhost/"
	url := "ws://localhost:8081"

	ws, err := websocket.Dial(url+"?connection_id="+id, "", origin)
	if err != nil {
		log.Fatalf("hix %v", err)
	}

	//if _, err := ws.Write([]byte("hello, world!\n")); err != nil {
	//log.Fatalf("hihi %v", err)
	//}
	for i := 0; i < 100; i++ {
		msg := make([]byte, 512)
		var n int
		if n, err = ws.Read(msg); err != nil {
			log.Fatalf("this %v", err)
		}
		if string(msg[:n]) != fmt.Sprintf("hello-%d", i) {
			t.Fatalf("wrong received message, shoud be '%s', got '%s'.", fmt.Sprintf("hello-%d", i), msg)
		}
	}
	if err := ws.Close(); err != nil {
		log.Fatalf("close err %v", err)
	}
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
		if err := mgr.Connect(r, w, cid, nil); err != nil {
			panic(err)
		}
	})
	go handleHttp(mgr)
	time.Sleep(100 * time.Millisecond)
	os.Exit(m.Run())
}

var mgr *Mgr

func TestNormal(t *testing.T) {
	deadChan := make(chan string, 1000)
	commitChan := make(chan int64, 1000)
	mgr = NewManager([]string{"localhost:6379"}, "", deadChan, commitChan)
	for i := 0; i < 10; i++ {
		id := toWsId(i)
		go func(id string) {
			url, origin := "ws://localhost:8081", "http://localhost/"
			ws, err := websocket.Dial(url+"?connection_id="+id, "", origin)
			if err != nil {
				log.Fatalf("hix %v", err)
			}

			for {
				msg := make([]byte, 512)
				var n int
				if n, err = ws.Read(msg); err != nil {
					log.Fatalf("this %v", err)
				}

				// fmt.Printf("got %s\n", msg)
				msg_split := strings.Split(string(msg[:n]), "-")
				if len(msg_split) != 2 {
					log.Fatalf("malform package %s", msg[:n])
				}
				// fmt.Println("commit", []byte(msg_split[1]))
				if _, err := ws.Write([]byte(msg_split[1])); err != nil {
					log.Fatalf("commit err %v", err)
				}
			}
		}(id)
	}

	time.Sleep(100 * time.Millisecond)

	for i := 0; i < 100; i++ {
		mgr.Send(toWsId(i%10), int64(i), []byte(fmt.Sprintf("solo-%d", i)))
	}

	committed_offsets := make([]int64, 0)
	for c := range commitChan {
		committed_offsets = append(committed_offsets, c)
		completed := true
		for i := int64(0); i < 100; i++ {
			found := false
			for _, c := range committed_offsets {
				if c == i {
					found = true
					break
				}
			}
			if !found {
				completed = false
				break
			}
		}
		if completed {
			break
		}
	}
}

func toWsId(i int) string { return fmt.Sprintf("0WERASDF-%d", i) }

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc %v | Total %v KiB | Sys %v KiB | %v Goroutines\n", bToKb(m.Alloc), bToKb(m.TotalAlloc), bToKb(m.Sys), runtime.NumGoroutine())
}

func bToKb(b uint64) uint64 {
	return b / 1024
}

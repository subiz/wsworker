package gobwas

import (
	"testing"
	 "net/http"
	"runtime"
	"fmt"
	"log"
	"runtime/debug"
	"sync"
	"time"
)

func TestGoroutine(t *testing.T) {
	lox := &sync.Mutex{}
	debug.SetGCPercent(-1)
	PrintMemUsage()
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		lox.Lock()
		defer lox.Unlock()
		ws, err := NewWs(w, r, nil)
		if err != nil {
			t.Fatal(err)
		}

		if err := ws.Send([]byte("Tran Thi Hai Van")); err != nil {
			t.Fatal(err)
		}

		for m := range ws.Recv() {
			println("got", string(m))
		}

    // fmt.Fprintf(w, "Hi there, I love %s!", r.URL.Path[1:])
	})

	go func() {
		for {
			runtime.GC()
			PrintMemUsage()
			time.Sleep(1 * time.Second)
		}
	}()
	log.Fatal(http.ListenAndServe(":8080", nil))
}


func bToKb(b uint64) uint64 {
	return b / 1024
}

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc %v | Total %v KiB | Sys %v KiB | %v Go\n", bToKb(m.Alloc), bToKb(m.TotalAlloc), bToKb(m.Sys), runtime.NumGoroutine())
}

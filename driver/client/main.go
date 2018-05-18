package main

import (
	"golang.org/x/net/websocket"
	"log"
	"fmt"
	"time"
	"sync"
)

var	lox = &sync.Mutex{}
var nc = 0
func hold(url, origin string) {
	nc++
	ws, err := websocket.Dial(url, "", origin)
	if err != nil {
    log.Fatalf("hix %v", err)
	}
	if _, err := ws.Write([]byte("hello, world!\n")); err != nil {

    log.Fatalf("hihi %v", err)
	}
	var msg = make([]byte, 512)
	var n int
	time.Sleep(10 * time.Second)
	if n, err = ws.Read(msg); err != nil {
		println("hihihix")
    log.Fatal(err)
	}
	fmt.Printf("%d Received: %s.\n", nc, msg[:n])
	for {
		if _, err := ws.Write([]byte("hello, world!\n")); err != nil {
			log.Fatal(err)
		}
		time.Sleep(10 * time.Second)
	}
}

func main() {
	for i := 0; i < 1; i++ {
		go hold( "ws://localhost:8080", "http://localhost/")
	}
	time.Sleep(100 * time.Second)
}

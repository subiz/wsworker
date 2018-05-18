// +build ignore

package main

import (
	"fmt"
	"runtime"
	"time"
)

type Ticker struct {
	C    chan bool
	stop bool
}

func (t *Ticker) Stop() {
	t.stop = true
}

func NewTicker(d time.Duration) *Ticker {
	t := &Ticker{
		C: make(chan bool),
	}
	go func() {
		for !t.stop {
			t.C <- true
			time.Sleep(d)
		}
	}()
	return t
}

func noTicker() {
	time.Sleep(1 * time.Hour)
}

func ticker() {

	//t := time.NewTicker(1 * time.Millisecond)
	t := NewTicker(1 * time.Millisecond)
	//loop:
	for {

		select {
		case <-t.C:
			time.Sleep(1 * time.Second)
			//println("here")
			//<-t.C
			//return
			//println("here2")
			//      break loop
			//case <- time.After(2 * time.Second):
			//println("timeout")
			//default:
			//println("default")
		}
	}
	time.Sleep(100 * time.Second)
}

func timeafter() {
	for {
		select {
		case <-time.After(1 * time.Millisecond):


		}
	}
}

func main() {
	PrintMemUsage()
	for i := 0; i < 1; i++ {
		go timeafter()
		//go ticker()
		//go noTicker()
	}

	for {
		PrintMemUsage()
		time.Sleep(200 * time.Millisecond)
	}
}

func bToKb(b uint64) uint64 {
	return b / 1024
}

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v KiB", bToKb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v KiB", bToKb(m.TotalAlloc))
	fmt.Printf("\tSys = %v KiB", bToKb(m.Sys))
	//fmt.Printf("\tNumGC = %v\n", m.NumGC)
	fmt.Printf("\tNumGo = %v\n", runtime.NumGoroutine())
}

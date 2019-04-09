package wsworker

import (
	"github.com/subiz/executor"
	"sync"
	"time"
)

// Map is a synchronized map
// we created this instead of using sync.Map because we want more efficient scan through
// the map
type Map struct {
	*sync.RWMutex // own map

	// hold map's keys and values
	m map[string]interface{}

	// hold expired keys with its expires time in second
	// note that the value is an unix timestamp, not a duration
	expires map[string]int64
}

// NewMap creates a new Map object
func NewMap() *Map {
	me := &Map{
		RWMutex: &sync.RWMutex{},
		m:       make(map[string]interface{}, 1000),
		expires: make(map[string]int64, 1000),
	}
	go me.clearExpired()
	return me
}

// clearExpires is a routines runs every 1 minute which delete all expired keys
func (me *Map) clearExpired() {
	for {
		now := time.Now().Unix()
		me.Lock()
		for k, exp_sec := range me.expires {
			if exp_sec < now {
				delete(me.m, k)
				delete(me.expires, k)
			}
		}
		me.Unlock()
		time.Sleep(1 * time.Minute)
	}
}

// Get lookups a value by key, return pair (nil, false) if not found
func (me *Map) Get(key string) (interface{}, bool) {
	me.RLock()
	value, has := me.m[key]
	me.RUnlock()
	return value, has
}

// GetOrCreate returns the matched value by key or insert a value returned
// by newf if not match.
// newf is called only when we dont found the key
func (me *Map) GetOrCreate(key string, newf func() interface{}) interface{} {
	me.Lock()
	value, has := me.m[key]
	if !has {
		value = newf()
		me.m[key] = value
	}
	me.Unlock()
	return value
}

// Set updates or inserts a <key, value> pair
func (me *Map) Set(key string, value interface{}) {
	me.Lock()
	me.m[key] = value
	delete(me.expires, key)
	me.Unlock()
}

// Expire marks the given key as 'safe to delete' and will be cleaned up after (about)
// exp_sec seconds
func (me *Map) Expire(key string, exp_sec int64) {
	now := time.Now().Unix()
	me.Lock()
	me.expires[key] = now + exp_sec
	me.Unlock()
}

// Delete removes key out of the map
func (me *Map) Delete(key string) {
	me.Lock()
	delete(me.m, key)
	delete(me.expires, key)
	me.Unlock()
}

// Scan loops through all workers parallelly without creating race condition
// on the map
func (me *Map) Scan(f func(key string, value interface{})) {
	exe := executor.New(100, 3, f)

	me.RLock()
	for k, v := range me.m {
		me.RUnlock()
		exe.Add(k, v)
		me.RLock()
	}
	me.RUnlock()
	exe.Wait()
}

package wsworker

import (
	"github.com/subiz/executor"
	"sync"
)

// Map is a synchronized map
// we created this instead of using sync.Map because we want more efficient scan through
// the map
type Map struct {
	*sync.RWMutex // own map
	m             map[string]interface{}
}

// NewMap creates a new Map object
func NewMap() Map {
	return Map{RWMutex: &sync.RWMutex{}, m: make(map[string]interface{}, 1000)}
}

// Get lookups a value by key, return pair (nil, false) if not found
func (me *Map) Get(key string) (interface{}, bool) {
	me.RLock()
	value, has := me.m[key]
	me.RUnlock()
	return value, has
}

// Set update or insert a <key, value> pair
func (me *Map) Set(key string, value interface{}) {
	me.Lock()
	me.m[key] = value
	me.Unlock()
}

// Delete removes key out of the map
func (me *Map) Delete(key string) {
	me.Lock()
	delete(me.m, key)
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

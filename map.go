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

	// holds map's keys and values
	m map[string]interface{}

	// maximum item can keep
	size int64

	exe *executor.GroupMgr
}

// NewMap creates a new Map object
func NewMap() Map {
	return Map{
		RWMutex: &sync.RWMutex{},
		m: make(map[string]interface{}, 1000),
		exe: executor.NewGroupMgr(1000),
	}
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
	gr := me.exe.NewGroup(f)
	me.RLock()
	for k, v := range me.m {
		me.RUnlock()
		gr.Add(k, v)
		me.RLock()
	}
	me.RUnlock()
	gr.Wait()
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

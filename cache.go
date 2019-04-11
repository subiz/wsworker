package wsworker

import (
	"container/ring"
	"github.com/subiz/goredis"
	"sync"
	"time"
)

// cacheItem represences an item inside the cache
type cacheItem struct {
	// tells whether the item is existed
	existed bool

	value []byte
}

// Cache is a simple LRU cache based on golang map and backed by redis
type Cache struct {
	*sync.Mutex

	// holds map's keys and values
	m map[string]cacheItem

	// maximum item can keep
	cap int

	// current number of item
	size int

	// the first item will be the oldest inserted key
	createdRing *ring.Ring

	// used to communicate with a redis cluster
	rclient *goredis.Client
}

// NewMap creates a new Map object
func NewCache(redis_hosts []string, password string, cap int) (*Cache, error) {
	me := &Cache{
		Mutex:       &sync.Mutex{},
		m:           make(map[string]cacheItem, cap),
		cap:         cap,
		createdRing: ring.New(cap),
	}

	rclient, err := goredis.New(redis_hosts, password)
	if err != nil {
		return nil, err
	}
	me.rclient = rclient
	return me, nil
}

// Get lookups a value by key, return pair (nil, false) if not found
func (me *Cache) Get(key string) ([]byte, bool, error) {
	me.Lock()
	defer me.Unlock()

	item, has := me.m[key]
	if has { // hit local cache
		return item.value, item.existed, nil
	}

	// local cache miss, lookup in redis
	value, has, err := me.rclient.Get(key, key) // may block
	if err != nil {
		return nil, false, err
	}

	if me.size+1 > me.cap { // overflow, removing oldest item
		oldest_key := me.createdRing.Next().Value.(string)
		delete(me.m, oldest_key)
		me.size--
	}
	me.size++
	me.createdRing = me.createdRing.Next()
	me.createdRing.Value = key
	me.m[key] = cacheItem{value: value, existed: has}
	return value, has, nil
}

// Set updates or inserts a <key, value> pair
func (me *Cache) Set(key string, value []byte) error {
	me.Lock()
	if _, has := me.m[key]; !has { // new item
		if me.size+1 > me.cap { // overflow, removing oldest item
			oldest_key := me.createdRing.Next().Value.(string)
			delete(me.m, oldest_key)
			me.size--
		}
		me.size++
		me.createdRing = me.createdRing.Next()
		me.createdRing.Value = key
	}
	me.m[key] = cacheItem{value: value, existed: true}

	// reset to redis
	err := me.rclient.Set(key, key, value, 24*time.Hour) // may block
	me.Unlock()
	return err
}
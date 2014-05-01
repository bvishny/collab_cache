package collabcache

import (
	"time"
)

// Type CollabCache provides a concurrent cache that prevents duplicate work
type CollabCache struct {
	cache       map[string]*CacheItem
	lock        *StripedRWLock // Used for striped locking on most operations
	initLock    *StripedRWLock // Used for striped locking specifically on key refresh
	cond        *StripedCond   // Used to notify dog-piled threads a refresh completed
	pqueue      *PriorityQueue // a thread-safe priority queue used to store expirations by key
	quitChannel chan bool      // Receives global quit message and shuts down invalidator
}

// Type CacheItem stores values, TTL, and lock times
type CacheItem struct {
	item     interface{}
	expires  int64 // Indicates time when item expires
	stale    int64 // Indicates time when item stale
	lockTime int64 // Indicates time item locked until
}

// Func NewCollabCache Creates a new CollabCache given quit channel for Invalidator
// A separate GoRoutine for Invalidator is also started
func NewCollabCache(quitChannel chan bool) *CollabCache {
	cache := &CollabCache{
		cache:       make(map[string]*CacheItem),
		lock:        NewStripedRWLock(DefaultLockBuckets, JavaHashFunc),
		initLock:    NewStripedRWLock(DefaultLockBuckets/2, JavaHashFunc),
		pqueue:      NewPriorityQueue(),
		quitChannel: quitChannel,
	}

	cache.cond = NewStripedCond(cache.lock, true)

	go cache.invalidator()
	return cache
}

// Func NewCacheItem Creates a new CacheItem
func NewCacheItem() *CacheItem {
	return &CacheItem{}
}

// Gets or Publishes value using generatorFunc if not available
// ttl, timeout is specified in milliseconds
// If refresh times out, nil will be returned
func (cc *CollabCache) GetOrPublish(key string, ttl int64, generatorFunc func(string) interface{}, timeout int64) interface{} {
	// lock for reading
	cc.lock.RLock(key)
	defer cc.lock.RUnlock(key)
	// If wait needed
	for !cc.canRead(key) {
		if cc.tryLock(key, timeout) {
			go cc.refresh(key, ttl, cc.cache[key].lockTime, generatorFunc)
		}
		cc.cond.Wait(key)
	}

	if cc.isStale(key) && cc.tryLock(key, timeout) {
		go cc.refresh(key, ttl, cc.cache[key].lockTime, generatorFunc)
	}

	return cc.cache[key].item
}

// Indicates whether a key is ready for reading - not threadsafe
func (cc *CollabCache) canRead(key string) bool {
	ci, ok := cc.cache[key]
	return ok && ci.expires > CurrentTimeInMillis()
}

// Indicates whether key locked for refresh by another thread - not threadsafe
func (cc *CollabCache) isLocked(key string) bool {
	ci, ok := cc.cache[key]
	return ok && ci.lockTime > CurrentTimeInMillis()
}

// Indicates whether key is stale
func (cc *CollabCache) isStale(key string) bool {
	ci, ok := cc.cache[key]
	return ok && ci.stale < CurrentTimeInMillis()
}

// Try to lock key (refresh) for timeout milliseconds, return true
func (cc *CollabCache) tryLock(key string, timeout int64) (success bool) {
	if cc.isLocked(key) {
		return
	}

	cc.initLock.Lock(key)
	defer cc.initLock.Unlock(key)

	if cc.isLocked(key) {
		return
	}

	// Initialize item if not initialized
	if _, ok := cc.cache[key]; !ok {
		cc.cache[key] = NewCacheItem()
	}

	cc.cache[key].lockTime = CurrentTimeInMillis() + timeout
	return true
}

// Write value to key if lock has not changed
// Evaluate scenario where lock not changed but timeout elapsed
func (cc *CollabCache) writeKey(key string, value interface{}, ttl int64, lockUntil int64, callback func(string)) {
	cc.lock.Lock(key)
	defer cc.lock.Unlock(key)

	// if lock has not expired
	if ci, ok := cc.cache[key]; ok && ci.lockTime == lockUntil {
		now := CurrentTimeInMillis()
		cc.cache[key].item = value
		cc.cache[key].expires = now + ttl
		cc.cache[key].stale = now + (ttl / 2)
		cc.cache[key].lockTime = now
		// Write to Priority Queue
		cc.pqueue.Push(key, int(now+ttl))
		// Callback
		callback(key)
	}
}

// Refresh key - blocking operation, requires prior lock
// Return value when operation finishes, even if another thread's value chosen
func (cc *CollabCache) refresh(key string, ttl int64, lockUntil int64, generatorFunc func(string) interface{}) {
	// Spin off goroutine to signal others if timeout elapsed and not written
	cancelSignalChan := make(chan bool, 1)

	go func() {
		timer := time.NewTimer(time.Duration(lockUntil-CurrentTimeInMillis()) * time.Millisecond)
		select {
		case <-timer.C:
			cc.cond.Broadcast(key)
		case <-cancelSignalChan:
			return
		}
	}()

	// cannot count on exact sleep
	value := generatorFunc(key)

	// Write key and cancel signal
	cc.writeKey(key, value, ttl, lockUntil, func(key string) {
		cancelSignalChan <- true
		cc.cond.Broadcast(key)
	})
}

// Attempt to invalidate a given key if not locked
func (cc *CollabCache) invalidate(key string) {
	if cc.isLocked(key) {
		return
	}

	cc.lock.Lock(key)
	defer cc.lock.Unlock(key)

	// Allow 10 ms for deletion - once deleted this doesn't matter
	if !cc.isLocked(key) && cc.tryLock(key, 10) {
		// Theoretically if it fell asleep we would have problem
		delete(cc.cache, key)
	}
}

// Standalone Goroutine that invalidates expired items every second
func (cc *CollabCache) invalidator() {
	ticker := time.NewTicker(time.Duration(1) * time.Second)

	for {
		select {
		case time := <-ticker.C:
			{
				now := time.Unix() * 1000

				// Priority is the expiration time. PQueue is sorted by lowest expiration time
				// Theoretically item could be different than the popped value. But popped value must have even lower expiration.
				// Since this thread is currently the sole Popper we also don't risk popping an empty PQueue
				for item := cc.pqueue.Peek(); item != nil && int64(item.priority) <= now; item = cc.pqueue.Peek() {
					cc.invalidate(cc.pqueue.Pop().value.(string)) // items are strings corresponding to key names
				}
			}
		case <-cc.quitChannel:
			return
		}
	}
}

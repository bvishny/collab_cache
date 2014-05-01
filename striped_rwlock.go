package collabcache

import (
	"sync"
)

const (
	DefaultLockBuckets = 8
)

// Var JavaHashFunc is the java.lang.String.hashCode
var JavaHashFunc func(string) uint32 = func(s string) uint32 {
	var val uint32 = 1
	for i := 0; i < len(s); i++ {
		val += (val * 37) + uint32(s[i])
	}
	return val
}

// Type StripedRWLock allows sharding of mutexes for different keys
type StripedRWLock struct {
	hashFunc   func(string) uint32
	numBuckets uint32
	buckets    []sync.RWMutex
}

// Func NewStripedRWLock creates a new Mutex with numBuckets buckets based on hashFunc
func NewStripedRWLock(numBuckets uint32, hashFunc func(string) uint32) *StripedRWLock {
	// Use java hashCode if none specified
	if hashFunc == nil {
		hashFunc = JavaHashFunc
	}

	return &StripedRWLock{
		hashFunc:   hashFunc,
		numBuckets: numBuckets,
		buckets:    make([]sync.RWMutex, numBuckets),
	}
}

// Lock the mutex responsible for key
func (sl *StripedRWLock) Lock(key string) {
	sl.lockFor(key).Lock()
}

// Unlock the mutex responsible for key
func (sl *StripedRWLock) Unlock(key string) {
	sl.lockFor(key).Unlock()
}

// Read Lock the mutex responsible for key
func (sl *StripedRWLock) RLock(key string) {
	sl.lockFor(key).RLock()
}

// Read Unlock the mutex responsible for key
func (sl *StripedRWLock) RUnlock(key string) {
	sl.lockFor(key).RUnlock()
}

// Return the lock for a given key
func (sl *StripedRWLock) lockFor(key string) *sync.RWMutex {
	return &sl.buckets[sl.hashFunc(key)%sl.numBuckets]
}

// Return info for Striped Cond Integration
func (sl *StripedRWLock) CondInfo() (locks []*sync.RWMutex, hashFunc func(string) uint32) {
	locks = make([]*sync.RWMutex, sl.numBuckets, sl.numBuckets)
	numBuckets := int(sl.numBuckets)
	for i := 0; i < numBuckets; i++ {
		locks[i] = &sl.buckets[i]
	}
	hashFunc = sl.hashFunc
	return
}

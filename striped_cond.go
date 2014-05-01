package collabcache

import (
	"sync"
)

// Type StripedCond provides a Striped Implementation of Cond
type StripedCond struct {
	locks      []*sync.RWMutex
	hashFunc   func(string) uint32
	numBuckets uint32
	conds      []*sync.Cond
}

// Type PseudoLocker wraps a RWMutex and implements Locker.
// If readOnly RLock is called on Lock
type PseudoLocker struct {
	readOnly bool
	lock     *sync.RWMutex
}

/// Func pl.Lock Locks or RLocks its mutex depending on readOnly
func (pl *PseudoLocker) Lock() {
	if pl.readOnly {
		pl.lock.RLock()
	} else {
		pl.lock.Lock()
	}
}

// Func pl.Unlock Unlocks or RUnlocks its mutex depending on readOnly
func (pl *PseudoLocker) Unlock() {
	if pl.readOnly {
		pl.lock.RUnlock()
	} else {
		pl.lock.Unlock()
	}
}

// Func NewStripedCond creates a new StripedCond given a StripedRWLock.
// RLock() used if readOnly
func NewStripedCond(rwlock *StripedRWLock, readOnly bool) *StripedCond {
	rwLocks, hashFunc := rwlock.CondInfo()
	numBuckets := len(rwLocks)
	conds := make([]*sync.Cond, numBuckets, numBuckets)

	for i := 0; i < numBuckets; i++ {
		conds[i] = sync.NewCond(&PseudoLocker{readOnly, rwLocks[i]})
	}

	return &StripedCond{
		locks:      rwLocks,
		hashFunc:   hashFunc,
		numBuckets: uint32(numBuckets),
		conds:      conds,
	}
}

// Func sc.condFor returns the Cond for a given Key
func (sc *StripedCond) condFor(key string) *sync.Cond {
	return sc.conds[sc.hashFunc(key)%sc.numBuckets]
}

// Func sc.Broadcast sends a signal to all waiters for a shard
func (sc *StripedCond) Broadcast(key string) {
	sc.condFor(key).Broadcast()
}

// Func sc.Signal sends a signal to a single for a shard
func (sc *StripedCond) Signal(key string) {
	sc.condFor(key).Signal()
}

// Func sc.Wait causes the current client to wait on shard
func (sc *StripedCond) Wait(key string) {
	sc.condFor(key).Wait()
}

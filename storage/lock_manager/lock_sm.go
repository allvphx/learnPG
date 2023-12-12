package lock_manager

import "sync"

// lock_sm: the lock data structures placed inside shared memory.

// FastPathStrongRelationLock BF like fast path lock
type FastPathStrongRelationLock struct {
	lock  sync.Mutex
	count []uint32
}

func NewFastPathSRL() *FastPathStrongRelationLock {
	return &FastPathStrongRelationLock{
		lock:  sync.Mutex{},
		count: make([]uint32, FastPathStrongRelationLockPartitions),
	}
}

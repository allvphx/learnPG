package lock_manager

import "sync/atomic"

type lwDebugger struct {
	NWaiters atomic.Uint32
}

// LWLock lightweight lock
type LWLock struct {
	tranche uint16
	state   atomic.Uint32
	waiters ProcListHead

	// debug fields
}

type LWLockMode uint8

const (
	Exclusive     = LWLockMode(0)
	Shared        = LWLockMode(1)
	WaitUntilFree = LWLockMode(2)
)

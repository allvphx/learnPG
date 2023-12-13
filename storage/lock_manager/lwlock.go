package lock_manager

import "sync/atomic"

type lwDebugger struct {
	NWaiters atomic.Uint32
	Owner    *Routine
}

// LWLock lightweight lock
type LWLock struct {
	Tranche uint16
	State   atomic.Uint32
	Waiters ProcListHead
	debug   *lwDebugger // debug fields
}

type NamedLWLockTranche struct {
	ID   int
	Name string
}

type LWLockMode uint8

const (
	Exclusive     = LWLockMode(0)
	Shared        = LWLockMode(1)
	WaitUntilFree = LWLockMode(2)
)

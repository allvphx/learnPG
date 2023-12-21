package lock_manager

import (
	"LearnPG/errf"
	"LearnPG/port"
	"runtime"
	"sync/atomic"
)

type LWLockMode uint8

const (
	// There should have a list of built-in locks and their marks.

	NumOfFixedLWLocks = 100
	LWFixedLockMark   = 0

	LWFlagHasWaiters = uint32(1 << 30)
	LWFlagCanRelease = uint32(1 << 29)
	LWFlagLocked     = uint32(1 << 28)

	LWValueExclusive = uint32(1 << 24)
	LWValueShared    = uint32(1)

	LWSharedMask = uint32((1 << 24) - 1) // mask 0~24 bits, share lock count.
	LWLockMask   = uint32((1 << 25) - 1) // also mask exclusive lock

	LWExclusive     = LWLockMode(0)
	LWShared        = LWLockMode(1)
	LWWaitUntilFree = LWLockMode(2)
)

type lwDebugger struct {
	NWaiters atomic.Uint32
	Owner    *Routine
}

// LWLock lightweight lock that protect internal state with spin-locks.
// Unfortunately the overhead of taking the spinlock proved to be too high for read-heavy workloads/locks.
// The lock manager continuously pay spinlock cost for share mode locks, even if it is free.
// Thus, LWLock in PG tries to implement wait-free shared lock acquisition for non-exclusively locked items.
// It implements a single atomic lockcount instead of separated shared/exclusive lock counters protected by spinlock,
// and uses a single atomic operation to acquire the lock.
// It supports:
// - exclusive mode and shared mode locking.
// - wait until a value change.
// - a lock wait queue to avoid starvation.
type LWLock struct {
	Tranche uint16
	State   atomic.Uint32
	Waiters *ProcListHead
	debug   *lwDebugger // debug fields
}

func (c *LWLock) Init(trancheID uint16) {
	c.State.Store(LWFlagCanRelease)
	c.Tranche = trancheID
	c.Waiters = NewProcList()
}

// LWLockReportWaitStart and LWLockReportWaitEnd routine wait profiling.
func (c *LWLock) LWLockReportWaitStart() {}
func (c *LWLock) LWLockReportWaitEnd()   {}

// lockFreeLWLockAttemptLock will not block waiting for a lock to become free - that's the caller's job.
// it will try to atomically acquire the lock in passed in mode.
func (c *LWLock) lockFreeLWLockAttemptLock(mode LWLockMode) bool {
	oldState := c.State.Load()
	for {
		lockFree := false
		desiredState := oldState
		// check lock compatibility with old state.
		if mode == LWExclusive {
			lockFree = (oldState & LWLockMask) == 0
			if lockFree {
				desiredState += LWValueExclusive
			}
		} else {
			lockFree = (oldState & LWValueExclusive) == 0
			if lockFree {
				desiredState += LWValueShared
			}
		}
		if c.State.CompareAndSwap(oldState, desiredState) {
			if lockFree { // succeed!
				return false
			} else {
				return true
			}
		}
		runtime.Gosched()
		// Alike the implementation of spin lock, we avoid busy loop by enabling context switching.
	}
}

// LWLockWaitListLock lock the wait list against concurrent activities from other Proc.
// Such mutex lock time should be short.
func (c *LWLock) LWLockWaitListLock(proc *Proc) {
	for {
		oldState := port.AtomicFetchOrUint32(&c.State, LWFlagLocked)
		if (oldState & LWFlagLocked) == 0 {
			// we are the first one to lock, succeed.
			break
		}
		delay := NewSpinDelayStatus(proc)
		for oldState&LWFlagLocked != 0 {
			// lock-free fetch failed, it indicates that we are likely to be in contented mode, use spin delay.
			err := delay.PerformSpinDelay()
			if err != nil {
				panic(err)
			}
			oldState = c.State.Load()
		}
	}
}

func (c *LWLock) LWLockWaitListUnlock() {
	oldState := port.AtomicFetchAndUint32(&c.State, ^LWFlagLocked)
	errf.Assert(oldState&LWFlagLocked != 0, "LWLockWaitListUnlock encounters unlocked item")
}

// LWLockWakeup wake up all lockers that could acquire the lock.
func (c *LWLock) LWLockWakeup(proc *Proc) {
	releasedWaiters := NewProcList()
	c.LWLockWaitListLock(proc)
	someOneWoken := false
	canRelease := true

	// push all waken procs into the waiter list
	for it := c.Waiters.NewMultableIterator(); it != nil; it = it.Next() {
		waiter := it.cur
		if someOneWoken && waiter.LWLockWaitMode == LWExclusive {
			continue
		}

		// release the current lock.
		c.Waiters.Delete(it)
		releasedWaiters.PushTail(it.cur)

		if waiter.LWLockWaitMode != LWWaitUntilFree {
			someOneWoken = true
			canRelease = false
		}

		if waiter.LWLockWaitMode == LWExclusive {
			// all following waiter will fail, so just stop looping
			break
		}
	}

	errf.Assert(releasedWaiters.IsEmpty() || c.State.Load()&LWFlagHasWaiters != 0,
		"it is useless to call the wake up, could due to waiter lock mis-usage or wakeup function mis-usage")

	// For current lock, update flags, and release lock.
	port.SpinStyleAtomicFetchAndModifyUint32(&c.State, func(x uint32) uint32 {
		// modify the release mark.
		if canRelease { // Nobody is waiting for current lock.
			x |= LWFlagCanRelease
		} else {
			x &= ^LWFlagCanRelease
		}

		// update the waiter mark
		if c.Waiters.IsEmpty() {
			x &= ^LWFlagHasWaiters
		}

		// release the waiter lock.
		x &= ^LWFlagLocked
		return x
	})

	for it := releasedWaiters.NewMultableIterator(); it != nil; it = it.Next() {
		releasedWaiters.Delete(it)

		port.WriteBarrier()
		// The waiter Proc shall be release only after its delete on list.
		// Otherwise, the proc may get blocked again due to another get lock and enter queue again.

		waiter := it.cur
		waiter.LWWaiting = false
		go func() {
			// use channel is wake up the related waiter.
			waiter.waitSem <- true
		}()
	}
}

// NamedLWLockTranche the LWLock tranche request for named tranche.
type NamedLWLockTranche struct {
	ID   uint16
	Name string
}

type NamedLWLockTrancheRequest struct {
	Name     string
	NumLocks int
}

type LWLockManager struct {
	latch                               *SpinLock
	LockNamedLockRequestAllowed         bool
	MainLWLockArray                     []*LWLock
	NamedLWLockTrancheArray             []*NamedLWLockTranche
	NamedLWLockTrancheRequestsArray     []*NamedLWLockTrancheRequest
	NumOfNamedLWLockTrancheRequests     int
	NumOfLWLockTrancheRequestsAllocated int
	LWLockCounter                       uint16
}

func (c *LWLockManager) CountLWLocksByNamedTranches() int {
	sumUp := 0
	for _, v := range c.NamedLWLockTrancheRequestsArray {
		sumUp += v.NumLocks
	}
	return sumUp
}

func (c *LWLockManager) Size() int {
	return NumOfFixedLWLocks + c.CountLWLocksByNamedTranches()
}

func (c *LWLockManager) NewTrancheId() uint16 {
	c.latch.Lock()
	defer c.latch.Unlock()
	c.LWLockCounter++
	return c.LWLockCounter
}

func (c *LWLockManager) InitLWLocks() {
	c.latch = NewSpinLock()
	c.MainLWLockArray = make([]*LWLock, c.Size())
	c.LWLockCounter = 0
	offset := 0
	for ; offset < NumOfFixedLWLocks; offset++ {
		tmp := &LWLock{}
		// to do: add information about the built-in lock tranches.
		tmp.Init(LWFixedLockMark)
		c.MainLWLockArray[offset] = tmp
	}
	if c.NumOfNamedLWLockTrancheRequests > 0 {
		c.NamedLWLockTrancheArray = make([]*NamedLWLockTranche, c.NumOfNamedLWLockTrancheRequests)
		for i := 0; i < c.NumOfNamedLWLockTrancheRequests; i++ {
			request := c.NamedLWLockTrancheRequestsArray[i]
			c.NamedLWLockTrancheArray[i].Name = request.Name
			c.NamedLWLockTrancheArray[i].ID = c.NewTrancheId()
			for j := 0; j < request.NumLocks; j++ {
				tmp := &LWLock{}
				tmp.Init(c.NamedLWLockTrancheArray[i].ID)
				c.MainLWLockArray[offset] = tmp
				offset++
			}
		}
	}
	errf.Assert(offset == c.Size(), "incorrect offset calculation in InitLWLocks")
}

func (c *LWLockManager) RegisterLWLockTranches() {}

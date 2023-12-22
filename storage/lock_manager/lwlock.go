package lock_manager

import (
	"LearnPG/errf"
	"LearnPG/port"
	"context"
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
	LWWaitUntilFree = LWLockMode(2) // a special mode used for proc's LWLockMode.
)

type lwDebugger struct {
	NWaiters atomic.Uint32
	Owner    *Routine
}

// LWLock lightweight lock that protect internal state with spin-locks.
// They are "lightweight" because they are designed to be more efficient than heavier locking mechanisms,
// especially for short operations.
// Unfortunately the overhead of taking the spinlock-based mutex proved to be too high for read-heavy workloads/locks.
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

// LWLockAttemptLock will not block waiting for a lock to become free - that's the caller's job.
// it will try to atomically acquire the lock in passed in mode.
func (c *LWLock) LWLockAttemptLock(mode LWLockMode) bool {
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
func (c *LWLock) LWLockWaitListLock(ctx context.Context) {
	for {
		oldState := port.AtomicFetchOrUint32(&c.State, LWFlagLocked)
		if (oldState & LWFlagLocked) == 0 {
			// we are the first one to lock, succeed.
			break
		}
		delay := NewSpinDelayStatus(ctx.Value(ContextCurrentProc).(*Proc))
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
func (c *LWLock) LWLockWakeup(ctx context.Context) {
	releasedWaiters := NewProcList()
	c.LWLockWaitListLock(ctx)
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
		go waiter.Broadcast()
	}
}

// LWLockQueueSelf add current Proc to the wait queue
func (c *LWLock) LWLockQueueSelf(ctx context.Context, mode LWLockMode) {
	proc := ctx.Value(ContextCurrentProc).(*Proc)
	if proc == nil {
		panic("cannot wait without proc")
	}
	if proc.LWWaiting {
		panic("queueing for lock while waiting on another one")
	}

	c.LWLockWaitListLock(ctx)
	// set the flag to protect the queue
	port.AtomicFetchOrUint32(&c.State, LWFlagHasWaiters)

	proc.LWWaiting = true
	proc.LWLockWaitMode = mode

	if mode == LWWaitUntilFree {
		// the wait until free is always placed at the front of the list.
		c.Waiters.PushHead(proc)
	} else {
		c.Waiters.PushTail(proc)
	}

	c.LWLockWaitListUnlock()
}

// LWLockDequeueSelf remove current Proc from the wait queue
func (c *LWLock) LWLockDequeueSelf(ctx context.Context) {
	found := false
	c.LWLockWaitListLock(ctx)
	proc := ctx.Value(ContextCurrentProc).(*Proc)

	for it := c.Waiters.NewMultableIterator(); it != nil; it = it.Next() {
		// find the proc and remove it.
		if it.cur == proc {
			found = true
			c.Waiters.Delete(it)
			break
		}
	}

	if c.Waiters.IsEmpty() {
		port.AtomicFetchAndUint32(&c.State, ^LWFlagHasWaiters)
	}

	c.LWLockWaitListUnlock()

	// dangerous zone: protected by wait semaphore on PROC.

	if found {
		// normal case.
		proc.LWWaiting = false
	} else {
		// someone else has dequeued current proc from waiter list and will wake up current proc.
		// Deal with the superfluous absorption of a wakeup !!!

		port.AtomicFetchOrUint32(&c.State, LWFlagCanRelease)
		proc.Wait()
		errf.Assert(!proc.LWWaiting, "the wait sem should only be obtained after the wakeup finish")
	}
}

// LWLockAcquire acquire a light weighted lock.
func (c *LWLock) LWLockAcquire(ctx context.Context, mode LWLockMode) bool {

	errf.Assert(mode == LWShared || mode == LWExclusive, "invalid lock")

	// The retry and loop design: according to PG document, the LW Lock is used to protect
	// short section of computations. The same lock may get acquired many times during a single CPU cycle,
	// even in the presence of contention.
	// OvO The efficiency of being able to reacquire the lock outweighs the
	// potential downside of occasionally "wasting" a process dispatch cycle when a process wake up but find the
	// lock is not available and has to wait for it again.

	noWait := true

	proc := ctx.Value(ContextCurrentProc).(*Proc)

	for {
		needWait := c.LWLockAttemptLock(mode)
		if !needWait { // success
			break
		}

		c.LWLockQueueSelf(ctx, mode)
		// by placing the PROC inside queue, we are guaranteed to be woken up when necessary.

		/// --- WOKEN UP ---
		// check again since when this PROC is woken up, something could have changed.
		needWait = c.LWLockAttemptLock(mode)
		if !needWait {
			// succeed!
			c.LWLockDequeueSelf(ctx)
			break
		}
		c.LWLockReportWaitStart()
		proc.Wait()
		errf.Assert(!proc.LWWaiting, "the wait sem should only be obtained after the wakeup finish")

		// TODO: release block?
		port.AtomicFetchOrUint32(&c.State, LWFlagCanRelease)
		c.LWLockReportWaitEnd()

		noWait = false
	}

	// TODO: update and maintain heldLWLock list.
	return noWait
}

// LWLockConditionAcquire if lock is not availible, return false with no side effect.
func (c *LWLock) LWLockConditionAcquire(ctx context.Context, mode LWLockMode) bool {
	errf.Assert(mode == LWShared || mode == LWExclusive, "invalid lock")
	mustWait := c.LWLockAttemptLock(mode)
	// TODO: maintain the held lw lock list
	// TODO: check if we need interrupt barrels.
	return mustWait
}

// LWLockAcquireOrWait acquire a lock, or wait until it is free.
func (c *LWLock) LWLockAcquireOrWait(ctx *context.Context, mode LWLockMode) bool {
	return false
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

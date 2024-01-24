package lock_manager

import (
	"LearnPG/errf"
	"LearnPG/port"
	"context"
	"fmt"
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

func (c *LWLock) Info(ctx context.Context) {
	fmt.Printf("PROC %d with lock flag %s\n", ctx.Value(ContextCurrentProc).(*Proc).ProcID, c.String())
}

func (c *LWLock) String() string {
	flags := c.State.Load()
	isExclusive := (flags & LWValueExclusive) == LWValueExclusive
	numShare := flags & LWSharedMask
	isLocked := (flags & LWFlagLocked) == LWFlagLocked
	canRelease := (flags & LWFlagCanRelease) == LWFlagCanRelease
	hasWaiters := (flags & LWFlagHasWaiters) == LWFlagHasWaiters
	waiter := "["
	for it := c.Waiters.NewMultableIterator(); it != nil; it = it.Next() {
		// wake all wait for value locks.
		waiter = waiter + fmt.Sprintf("%s,", it.cur.String())
	}
	waiter += "]"
	//errf.Jprint(waiter)
	return fmt.Sprintf("{tranche:%d,has_waiters:%v,can_release:%v,is_locked:%v,is_write:%v,read_count:%d,water_list:%s}",
		c.Tranche,
		hasWaiters,
		canRelease,
		isLocked,
		isExclusive,
		numShare,
		waiter,
	)
}

func (c *LWLock) Init(trancheID uint16) {
	c.State.Store(LWFlagCanRelease)
	c.Tranche = trancheID
	c.Waiters = NewProcList()
}

// LWLockAttemptLock will not block waiting for a lock to become free - that's the caller's job.
// it will try to atomically acquire the lock in passed in mode.
func (c *LWLock) LWLockAttemptLock(mode LWLockMode) bool {
	for {
		oldState := c.State.Load()
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
	waitCount := 0

	proc := ctx.Value(ContextCurrentProc).(*Proc)

	fmt.Printf("PROC %d, --------------------------- START ---------------------\n", proc.ProcID)

	for {
		//fmt.Printf("PROC %v, current lock is %v\n", proc.ProcID, c.String())
		needWait := c.LWLockAttemptLock(mode)
		if !needWait { // success
			fmt.Printf("PROC %v, succeed with flags %v\n", proc.ProcID, c.String())
			break
		}

		c.lockQueueSelf(ctx, mode)
		fmt.Printf("PROC %d, fail for flags %s\n", proc.ProcID, c.String())
		// by placing the PROC inside queue, we are guaranteed to be woken up when necessary.

		/// --- WOKEN UP ---
		// check again since when this PROC is woken up, something could have changed.
		needWait = c.LWLockAttemptLock(mode)
		if !needWait {
			// succeed!
			fmt.Printf("PROC %v, succeed with flags %v\n", proc.ProcID, c.String())
			c.lockDequeueSelf(ctx)
			break
		}
		//c.Info()
		c.lockReportWaitStart()
		for {
			//c.Info(ctx)
			proc.SemaphoreLock()
			if !proc.LWWaiting {
				fmt.Printf("PROC %v, got sem!! (%v round(s))\n", proc.ProcID, waitCount)
				break
			}
			waitCount++
			fmt.Printf("PROC %v, waiting!! (%v round(s))\n", proc.ProcID, waitCount)
		}
		//proc.Wait()
		errf.Assert(!proc.LWWaiting, "the wait sem should only be obtained after the wakeup finish")

		// TODO: release block?
		port.AtomicFetchOrUint32(&c.State, LWFlagCanRelease)
		c.lockReportWaitEnd()

		noWait = false
	}

	proc.Broadcast()

	fmt.Printf("PROC %d, --------------------------- FIN ---------------------\n", proc.ProcID)

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

func (c *LWLock) LWLockReleaseMode(ctx context.Context, mode LWLockMode) {
	var old uint32

	fmt.Printf("PROC %v, releasing lock flags %v\n",
		ctx.Value(ContextCurrentProc).(*Proc).ProcID, c.String())
	if mode == LWExclusive {
		old = port.AtomicFetchSubUint32(&c.State, LWValueExclusive)
		if old&LWValueExclusive == 0 {
			panic("invalid exclusive lock release")
		}
	} else if mode == LWShared {
		old = port.AtomicFetchSubUint32(&c.State, LWValueShared)
		if old&LWValueShared == 0 {
			panic("invalid shared lock release")
		}
	} else {
		panic("invalid mode to be released by current PROC")
	}
	old = c.State.Load()

	if (old&LWFlagHasWaiters != 0) && (old&LWFlagCanRelease != 0) &&
		(old&LWLockMask == 0) {
		// if the current lock has pending waitors & no concurrent waitor list scan & the lock has been clean
		// we wake up the procs in the waitor list.
		c.lockWakeup(ctx)
	}
}

///////////////////////// Lock wait for values related APIs ////////////////////////////////

// LWLockAcquireOrWait acquire a lock, or wait until it is free and DO NOT lock.
// the logic is strange, and it seems only needed for WALWriteLock.
// to be implemented with write lock and page buffer.
func (c *LWLock) LWLockAcquireOrWait(ctx *context.Context, mode LWLockMode) bool {
	return false
}

// LWLockConflictsWithVar does the lock need to wait for value change, return Is lock free?
// If current PROC still needs to wait?
// case 3: we still need to wait for the update from lock holder. false, true
func (c *LWLock) LWLockConflictsWithVar(ctx context.Context, oldValue uint64, value *uint64) (curValue uint64,
	lockFree bool, needWait bool) {
	// check the state atomically but not lock for it?
	curValue = 0
	if c.State.Load()&LWValueExclusive == 0 {
		// case 1: current data is not locked, we directly return.
		lockFree = true
		needWait = false
		return
	}
	lockFree = false
	c.waitListLock(ctx)
	//case 2: current data is locked, but we do not need to wait since the data has changed since last time we saw it.
	// --> false, false
	curValue = *value
	c.waitListUnlock()
	if curValue != oldValue {
		return
	} else {
		needWait = true
		return
	}
}

// LWLockWaitForVar wait until the lock is free, or the value has been updated.
func (c *LWLock) LWLockWaitForVar(ctx context.Context, oldValue uint64, value *uint64) (curValue uint64, res bool) {
	proc := ctx.Value(ContextCurrentProc).(*Proc)
	res = false
	needWait := false
	// interrupts barrel.

	for {
		curValue, _, needWait = c.LWLockConflictsWithVar(ctx, oldValue, value)
		if !needWait {
			break
		}

		///////////// BEGIN The code block is twice attempt.
		c.lockQueueSelf(ctx, LWWaitUntilFree)
		// ??? the can release flag can ensure current proc is woken as soon as the lock is released.
		// How.
		port.AtomicFetchOrUint32(&c.State, LWFlagCanRelease)
		curValue, _, needWait = c.LWLockConflictsWithVar(ctx, oldValue, value)
		if !needWait {
			c.waitListUnlock()
			break
		}
		c.lockReportWaitStart()
		proc.Wait()
		c.lockReportWaitEnd()
		///////////// END the twice attempt code block.
		runtime.Gosched()
	}
	return
}

// LWLockUpdateVar the caller update the value and wake up all PROCs all call LWLockWaitForVar.
func (c *LWLock) LWLockUpdateVar(ctx context.Context, value *uint64, newValue uint64) {
	wakeUpList := NewProcList()
	c.waitListLock(ctx)
	errf.Assert(c.State.Load()&LWValueExclusive != 0,
		"the function LWLockUpdateVar can only be called with exclusive lock")
	*value = newValue
	// the wait until free waiters are placed at link head.
	for it := c.Waiters.NewMultableIterator(); it != nil; it = it.Next() {
		waiter := it.cur
		if waiter.LWLockWaitMode != LWWaitUntilFree {
			break
		}
		// wake all wait for value locks.
		c.Waiters.Delete(it)
		wakeUpList.PushTail(it.cur)
	}
	c.waitListUnlock()

	for it := wakeUpList.NewMultableIterator(); it != nil; it = it.Next() {
		wakeUpList.Delete(it)

		port.WriteBarrier()
		// The waiter Proc shall be release only after its delete on list.
		// Otherwise, the proc may get blocked again due to another get lock and enter queue again.

		waiter := it.cur
		waiter.LWWaiting = false
		waiter.Broadcast()
	}
}

func (c *LWLock) LWLockReleaseClearVar(ctx context.Context, value *uint64, resetValue uint64) {
	c.waitListLock(ctx)
	// why the change of value needs the lock of wait list?
	*value = resetValue
	c.waitListUnlock()
	c.LWLockRelease()
}

///////////////////////// LWLock inner APIs: lock info maintain ////////////////////////////////

// waitListLock lock the wait list against concurrent activities from other Proc.
// Such mutex lock time should be short.
func (c *LWLock) waitListLock(ctx context.Context) {
	for {
		oldState := port.AtomicFetchOrUint32(&c.State, LWFlagLocked)
		if (oldState & LWFlagLocked) == 0 {
			// we are the first one to lock, succeed.
			break
		}
		delay := NewSpinDelayStatus(ctx.Value(ContextCurrentProc).(*Proc))
		//c.Info()
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

func (c *LWLock) waitListUnlock() {
	oldState := port.AtomicFetchAndUint32(&c.State, ^LWFlagLocked)
	errf.Assert(oldState&LWFlagLocked != 0, "waitListUnlock encounters unlocked item")
}

// lockWakeup wake up all lockers that could acquire the lock.
func (c *LWLock) lockWakeup(ctx context.Context) {
	releasedWaiters := NewProcList()
	c.waitListLock(ctx)
	fmt.Printf("PROC %v, waking up waitors %v\n",
		ctx.Value(ContextCurrentProc).(*Proc).ProcID, c.String())
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
		waiter.latch.Lock()
		waiter.LWWaiting = false
		waiter.latch.Unlock()
		fmt.Printf("PROC %d waking up PROC %d\n", ctx.Value(ContextCurrentProc).(*Proc).ProcID,
			waiter.ProcID)
		waiter.Broadcast()
	}
}

// lockQueueSelf add current Proc to the wait queue
func (c *LWLock) lockQueueSelf(ctx context.Context, mode LWLockMode) {
	proc := ctx.Value(ContextCurrentProc).(*Proc)
	if proc == nil {
		panic("cannot wait without proc")
	}
	if proc.LWWaiting {
		panic("queueing for lock while waiting on another one")
	}

	c.waitListLock(ctx)
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

	c.waitListUnlock()
}

// lockDequeueSelf remove current Proc from the wait queue
func (c *LWLock) lockDequeueSelf(ctx context.Context) {
	found := false
	c.waitListLock(ctx)
	proc := ctx.Value(ContextCurrentProc).(*Proc)

	for it := c.Waiters.NewMultableIterator(); it != nil; it = it.Next() {
		// find the proc and remove it.
		if it.cur.ProcID == proc.ProcID {
			found = true
			c.Waiters.Delete(it)
			break
		}
	}

	if c.Waiters.IsEmpty() {
		port.AtomicFetchAndUint32(&c.State, ^LWFlagHasWaiters)
	}

	c.waitListUnlock()

	// dangerous zone: protected by wait semaphore on PROC.

	if found {
		// normal case.
		proc.LWWaiting = false
	} else {
		// someone else has dequeued current proc from waiter list and will wake up current routine.
		// Golang does not need to deal with signal absorb, just return.

		port.AtomicFetchOrUint32(&c.State, LWFlagCanRelease)
	}
}

// lockReportWaitStart and lockReportWaitEnd routine wait profiling.
func (c *LWLock) lockReportWaitStart() {}
func (c *LWLock) lockReportWaitEnd()   {}

// for test only, this api does not introduce the lock waiting overhead.
func (c *LWLock) lockReleaseNoWakeUp(mode LWLockMode) {
	var old uint32
	if mode == LWExclusive {
		old = port.AtomicFetchSubUint32(&c.State, LWValueExclusive)
		if old&LWValueExclusive == 0 {
			panic("invalid exclusive lock release")
		}
	} else if mode == LWShared {
		old = port.AtomicFetchSubUint32(&c.State, LWValueShared)
		if old&LWSharedMask == 0 {
			panic("invalid shared lock release")
		}
	} else {
		panic("invalid mode to be released by current PROC")
	}
}

///////////////////////// Held lock related APIs ////////////////////////////////

func (c *LWLock) LWLockRelease() {
	// check for the held lock stack and release the newest lock held for the current LWLock.
}

// LWLockReleaseAll release all locks held by current PROC. This is used for the lock cleaning after error.
func (c *LWLock) LWLockReleaseAll() {
}

// LWLockHeldByMe if the current lock is held by current PROC.
func (c *LWLock) LWLockHeldByMe() bool {
	return false
}

func (c *LWLock) LWLockHeldByMeInMode(mode LWLockMode) bool {
	return false
}

///////////////////////// Lock tranche related APIs ////////////////////////////////

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

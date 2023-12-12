package lock_manager

import (
	"LearnPG/errf"
	"runtime"
	"sync"
	"sync/atomic"
)

type SpinLockType uint8

const (
	BusyLoop            = SpinLockType(0)
	NoBusyLoop          = SpinLockType(1) // if we enable the explicit context switch for busy-loop goroutines, reducing CPU consumption.
	ChannelBased        = SpinLockType(2)
	Native              = SpinLockType(3) // native sync.Mutex
	DefaultSpinLockType = NoBusyLoop      // after benchmarking, the NoBusyLoop is the best one.
)

// SpinLock is generally CPU-intensive as it continuously poll a condition.
// They are only efficient in scenarios where the wait time is extremely short and
// the overhead of suspending and resuming a thread (like in a traditional mutex lock) is higher than the spin.
// In Golang, the scheduling time is smaller and the spin lock is less a reasonable design.
// From document: Share memory by communicating; don't communicate by sharing memory.
// This implementation is just a benchmark to investigate the performance of spin lock in the context of Golang.
type SpinLock struct {
	state    atomic.Uint32
	lockType SpinLockType
	ch       chan struct{}
	lc       *sync.Mutex
}

func NewSpinLock() *SpinLock {
	return NewSpinLockWithType(DefaultSpinLockType)
}

func NewSpinLockWithType(lockType SpinLockType) *SpinLock {
	res := SpinLock{lockType: lockType}
	if lockType == ChannelBased {
		res.ch = make(chan struct{}, 1)
	} else if lockType == Native {
		res.lc = &sync.Mutex{}
	} else {
		res.state = atomic.Uint32{}
	}
	return &res
}

func (c *SpinLock) Lock() {
	if c.lockType == ChannelBased {
		c.ch <- struct{}{}
	} else if c.lockType == Native {
		c.lc.Lock()
	} else {
		for !c.state.CompareAndSwap(0, 1) {
			if c.lockType == NoBusyLoop {
				runtime.Gosched()
			}
		}
	}
}

func (c *SpinLock) Unlock() {
	if c.lockType == ChannelBased {
		<-c.ch
	} else if c.lockType == Native {
		c.lc.Unlock()
	} else {
		errf.Assert(c.state.CompareAndSwap(1, 0),
			"Spin lock error: the spin lock is released without being locked")
	}
}

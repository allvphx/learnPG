package lock_manager

import (
	"LearnPG/errf"
	"LearnPG/port"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type SpinLockType uint8

const (
	BusyLoop            = SpinLockType(0)
	NoBusyLoop          = SpinLockType(1) // if we enable the explicit context switch for busy-loop goroutines, reducing CPU consumption.
	ChannelBased        = SpinLockType(2)
	Native              = SpinLockType(3) // native sync.Mutex
	PGContentedLock     = SpinLockType(4) // native sync.Mutex
	DefaultSpinLockType = NoBusyLoop
	// after benchmarking, the NoBusyLoop is the best one for short instruction cases.

	DefaultSpinsPerDelay  = 100
	MinNumOfSpinPerDelays = 10
	MaxNumOfSpinPerDelays = 100
	SpinMinDelay          = time.Millisecond
	SpinMaxDelay          = time.Second
)

var GlobalSpinProc = &Proc{
	SpinsPerDelay: DefaultSpinsPerDelay,
}

// SpinLock is generally CPU-intensive as it continuously poll a condition.
// They are only efficient in scenarios where the wait time is extremely short and
// the overhead of suspending and resuming a thread (like in a traditional mutex lock) is higher than the spin.
// In Golang, the scheduling time is smaller and the spin lock is less a reasonable design.
// From document: Share memory by communicating; don't communicate by sharing memory.
// This implementation is just a benchmarkSpinLock to investigate the performance of spin lock in the context of Golang.
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
	} else if lockType == PGContentedLock {
		res.state = atomic.Uint32{}
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
	} else if c.lockType == PGContentedLock {
		delay := NewSpinDelayStatus(GlobalSpinProc)
		for !c.state.CompareAndSwap(0, 1) {
			// lock-free fetch failed, it indicates that we are likely to be in contented mode, use spin delay.
			err := delay.PerformSpinDelay()
			if err != nil {
				panic(err)
			}
		}
		delay.FinishSpinDelay()
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

type SpinDelayStatus struct {
	Spins    int
	Delays   time.Duration
	CurDelay time.Duration
	from     *Proc
}

func NewSpinDelayStatus(c *Proc) *SpinDelayStatus {
	res := &SpinDelayStatus{
		Spins:    c.SpinsPerDelay,
		Delays:   0,
		CurDelay: 0,
		from:     c,
	}
	return res
}

// PerformSpinDelay wait while spinning on a contended spinlock.
// This is only used for contented locks!
func (c *SpinDelayStatus) PerformSpinDelay() error {
	runtime.Gosched() // CPU-specific delay each time.
	c.Spins++
	if c.Spins >= c.from.SpinsPerDelay {
		c.Delays++
		if c.Delays > MaxNumOfSpinPerDelays {
			return errf.ErrSpinStuck
		}
		if c.CurDelay == 0 {
			c.CurDelay = SpinMinDelay
		}
		time.Sleep(c.CurDelay)
		c.CurDelay += time.Duration(float64(c.CurDelay)*rand.Float64() + 0.5)
		// 2x - 2.5x, add random to avoid repeat the block between locks.
		if c.CurDelay > SpinMaxDelay {
			// Avoid waiting for too long.
			c.CurDelay = SpinMinDelay
		}
		c.Spins = 0
	}
	return nil
}

// FinishSpinDelay after managing to get the lock, we shall update on how long to loop.
// Higher spin per delay indicates that we are confident the contention is low.
// In case where the lock is "Acquired without delay", then we are good in parallel (low contention).
// Otherwise, our contention is high, and we are in serial execution mode.
func (c *SpinDelayStatus) FinishSpinDelay() {
	if c.CurDelay == 0 {
		// In case of no delay, we increase it rapidly since no delay indications multiprocessing mode.
		if c.from.SpinsPerDelay < MaxNumOfSpinPerDelays {
			c.from.SpinsPerDelay = port.MinInt(c.from.SpinsPerDelay+100, MaxNumOfSpinPerDelays)
		}
	} else {
		// In case of delay, we decrease it slowly since delay indications (possible) serial mode.
		if c.from.SpinsPerDelay > MaxNumOfSpinPerDelays {
			c.from.SpinsPerDelay = port.MaxInt(c.from.SpinsPerDelay-1, MinNumOfSpinPerDelays)
		}
	}
}

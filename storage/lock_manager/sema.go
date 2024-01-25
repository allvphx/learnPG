package lock_manager

import (
	"LearnPG/errf"
	"sync"
)

// Semaphore In Golang, signal or broadcast are lost if no one is waiting.
// This could be unexpected for some cases.
// Thus, we implement a sema that ensure signals are not lost.
type Semaphore struct {
	savedSignal uint
	latch       *sync.Mutex
	sem         *sync.Cond
	from        *Proc
}

func NewSemaphore(latch *sync.Mutex, from *Proc) *Semaphore {
	res := &Semaphore{
		savedSignal: 0,
		latch:       latch,
		from:        from,
	}
	if res.latch == nil {
		res.latch = &sync.Mutex{}
	}
	res.sem = sync.NewCond(res.latch)
	return res
}

func (c *Semaphore) Lock() {
	c.latch.Lock()
	defer c.latch.Unlock()
	if c.savedSignal > 0 {
		c.from.Debug("found old sem")
		errf.Assert(c.savedSignal > 0, "semaphore error: invalid lock sema")
		c.savedSignal--
	} else {
		c.from.Debug("waiting for sem")
		c.sem.Wait()
		c.from.Debug("got waited sem")
		errf.Assert(c.savedSignal > 0, "semaphore error: invalid lock sema")
		c.savedSignal--
	}
}

func (c *Semaphore) UnLock() {
	c.latch.Lock()
	c.savedSignal++
	c.latch.Unlock()
	c.from.Debug("sending sem")
	c.sem.Signal()
}

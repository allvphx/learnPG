package lock_manager

import "sync"

// Proc storage the local variables inside a goroutine.
type Proc struct {
	latch          *sync.Mutex
	sem            *sync.Cond
	SpinsPerDelay  int
	LWLockWaitMode LWLockMode
	LWWaiting      bool
}

func (c *Proc) Broadcast() {
	c.latch.Lock()
	defer c.latch.Unlock()
	c.sem.Broadcast()
}

func (c *Proc) Wait() {
	c.latch.Lock()
	defer c.latch.Unlock()
	for c.LWWaiting {
		c.sem.Wait()
	}
}

type ProcGlobal struct {
	AllProc []*Proc
}

var GlobalProc *ProcGlobal = nil

func (c *ProcGlobal) GetProc(i int) *Proc {
	return c.AllProc[i]
}

func NewProc() *Proc {
	res := &Proc{
		SpinsPerDelay:  DefaultSpinsPerDelay,
		LWLockWaitMode: LWWaitUntilFree,
		LWWaiting:      false,
		latch:          &sync.Mutex{},
	}
	res.sem = sync.NewCond(res.latch)
	return res
}

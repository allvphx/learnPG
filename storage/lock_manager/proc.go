package lock_manager

import (
	"LearnPG/errf"
	"fmt"
	"sync"
)

// Proc storage the local variables inside a goroutine.
type Proc struct {
	ProcID         uint32
	latch          *sync.Mutex
	sem            *sync.Cond
	SpinsPerDelay  int
	LWLockWaitMode LWLockMode
	LWWaiting      bool
}

func (c *Proc) String() string {
	c.latch.Lock()
	defer c.latch.Unlock()
	return errf.AsJson(c)
}

func (c *Proc) SemaphoreLock() {
	c.latch.Lock()
	defer c.latch.Unlock()
	fmt.Printf("PROC %v requests for sem.\n", c.ProcID)
	c.sem.Wait()
	fmt.Printf("PROC %v gets sem to continue.\n", c.ProcID)
}

func (c *Proc) SemaphoreUnlock() {
	//c.latch.Lock()
	//defer c.latch.Unlock()
	fmt.Printf("PROC %v add sem.\n", c.ProcID)
	c.sem.Signal()
}

func (c *Proc) Broadcast() {
	fmt.Printf("PROC %v broadcasting signal!!\n", c.ProcID)
	c.sem.Broadcast()
}

func (c *Proc) Wait() {
	c.latch.Lock()
	defer c.latch.Unlock()
	for c.LWWaiting {
		fmt.Printf("Blocked on PROC %v\n", c.ProcID)
		c.sem.Wait()
	}
	fmt.Printf("Blocked released wow!!!! %v\n", c.ProcID)
}

type ProcGlobal struct {
	AllProc []*Proc
}

var globalProc *ProcGlobal = nil
var globalProcLatch sync.Mutex
var globalProcID uint32 = 0

func (c *ProcGlobal) GetProc(i int) *Proc {
	return c.AllProc[i]
}

func NewProc() *Proc {
	globalProcLatch.Lock()
	defer globalProcLatch.Unlock()
	res := &Proc{
		SpinsPerDelay:  DefaultSpinsPerDelay,
		LWLockWaitMode: LWWaitUntilFree,
		LWWaiting:      false,
		latch:          &sync.Mutex{},
		ProcID:         globalProcID,
	}
	globalProcID++
	res.sem = sync.NewCond(res.latch)
	if globalProc == nil {
		globalProc = &ProcGlobal{
			AllProc: make([]*Proc, 0),
		}
	}
	globalProc.AllProc = append(globalProc.AllProc, res)
	return res
}

package lock_manager

import (
	"LearnPG/errf"
	"fmt"
	"strconv"
	"sync"
	"time"
)

// Proc storage the local variables inside a goroutine.
type Proc struct {
	ProcID         uint32
	latch          *sync.Mutex
	Sem            *Semaphore
	SpinsPerDelay  int
	LWLockWaitMode LWLockMode
	LWWaiting      bool
}

func (c *Proc) Debug(format string, a ...interface{}) {
	if !errf.EnableProcDebug {
		return
	}
	curFlag := c.String()
	fmt.Printf(time.Now().Format("15:04:05.00")+"; "+
		"PROC "+strconv.FormatUint(uint64(c.ProcID), 10)+":"+format+
		"; proc_flags="+curFlag+"\n", a...)
}

func (c *Proc) SetWaiting(to bool) {
	//c.latch.Lock()
	//defer c.latch.Unlock()
	c.LWWaiting = to
}

func (c *Proc) IsWaiting() bool {
	//c.latch.Lock()
	//defer c.latch.Unlock()
	return c.LWWaiting
}

func (c *Proc) String() string {
	//c.latch.Lock()
	//defer c.latch.Unlock()
	return fmt.Sprintf("{pid:%v,sem_cnt:%v,spin_per_delay:%v,wait_mode:%v}",
		c.ProcID,
		c.Sem.savedSignal,
		c.SpinsPerDelay,
		c.LWLockWaitMode.String())
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
	res.Sem = NewSemaphore(res.latch, res)
	if globalProc == nil {
		globalProc = &ProcGlobal{
			AllProc: make([]*Proc, 0),
		}
	}
	globalProc.AllProc = append(globalProc.AllProc, res)
	return res
}

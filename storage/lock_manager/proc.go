package lock_manager

// Proc storage the local variables inside a goroutine.
type Proc struct {
	SpinsPerDelay  int
	LWLockWaitMode LWLockMode
	LWWaiting      bool
	waitSem        chan bool
}

type ProcGlobal struct {
	AllProc []*Proc
}

var GlobalProc *ProcGlobal = nil

func (c *ProcGlobal) GetProc(i int) *Proc {
	return c.AllProc[i]
}

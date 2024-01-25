package lock_manager

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestLWLockAttemptLock(t *testing.T) {
	c := &LWLock{}
	c.Init(0)

	// the lock attempt functions shall block conflict locking requests.
	// share -- share compatible.

	assert.False(t, c.LWLockAttemptLock(LWShared))
	assert.False(t, c.LWLockAttemptLock(LWShared))
	assert.True(t, c.LWLockAttemptLock(LWExclusive))
	c.lockReleaseNoWakeUp(LWShared)
	c.lockReleaseNoWakeUp(LWShared)

	// the lock release shall succeed.
	assert.False(t, c.LWLockAttemptLock(LWExclusive))
	// check conflicts
	assert.True(t, c.LWLockAttemptLock(LWShared))
	assert.True(t, c.LWLockAttemptLock(LWExclusive))
}

func benchmarkLWLock(locker *LWLock, nRoutine, nOperation, opLen int) {
	wg := sync.WaitGroup{}
	start := time.Now()
	sharedValue := 0
	for i := 0; i < nRoutine; i++ {
		wg.Add(1)
		go func(op int, lock *LWLock, value *int) {
			//s := time.Now()
			proc := NewProc()
			ctx := context.WithValue(context.Background(), ContextCurrentProc, proc)
			randGen := rand.NewSource(int64(op))
			for j := 0; j < nOperation; j++ {
				mode := LWExclusive //LWLockMode(1 - (rand.Int()%10)/8)
				locker.LWLockAcquire(ctx, mode)
				for k := 0; k < opLen; k++ {
					*value += int(randGen.Int63() * (randGen.Int63()%3 - 1))
				}
				locker.LWLockReleaseMode(ctx, mode)
			}
			fmt.Printf("PROC %d done.\n", proc.ProcID)

			wg.Done()
		}(i, locker, &sharedValue)
	}

	wg.Wait()
	cost := time.Since(start).Seconds()
	fmt.Printf("Throughput is %.2f op/s\n", float64(nRoutine)*float64(nOperation)/cost)
}

func TestLWLockAcquire(t *testing.T) {
	concurrent, batch, instructs := 100, 100, 10
	c := &LWLock{}
	c.Init(0)
	benchmarkLWLock(c, concurrent, batch, instructs)
}

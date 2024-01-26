package lock_manager

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"sync"
	"sync/atomic"
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

func benchmarkLWLockAcquire(locker *LWLock, nRoutine, nOperation, opLen int, readPercentage float64) {
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
				if rand.Float64() <= readPercentage {
					mode = LWShared
				}
				locker.LWLockAcquire(ctx, mode)
				readSum := 0
				for k := 0; k < opLen; k++ {
					if mode == LWExclusive {
						*value += int(randGen.Int63() * (randGen.Int63()%3 - 1))
					} else {
						readSum += *value
					}
				}
				locker.LWLockReleaseMode(ctx, mode)
			}
			//fmt.Printf("PROC %d done.\n", proc.ProcID)
			wg.Done()
		}(i, locker, &sharedValue)
	}

	wg.Wait()
	cost := time.Since(start).Seconds()
	fmt.Printf("Throughput is %.2f op/s\n", float64(nRoutine)*float64(nOperation)/cost)
}

// Even in exclusive mode, LW lock is better than all types
// of spin locks for long instructions (close to channel lock).
// Its advantage becomes larger when considering the read operations.
func TestLWLockAcquire(t *testing.T) {
	concurrent, batch, instructs := 32, 100, 1000
	c := &LWLock{}
	c.Init(0)
	benchmarkLWLockAcquire(c, concurrent, batch, instructs, 0)
	benchmarkLWLockAcquire(c, concurrent, batch, instructs, 0.5)
	benchmarkLWLockAcquire(c, concurrent, batch, instructs, 1)
}

func benchmarkLWLockAcquireConditional(locker *LWLock, nRoutine, nOperation, opLen int, readPercentage float64) {
	wg := sync.WaitGroup{}
	start := time.Now()
	sharedValue := 0
	totalCount := int32(0)
	for i := 0; i < nRoutine; i++ {
		wg.Add(1)
		go func(op int, lock *LWLock, value *int) {
			//s := time.Now()
			operationCount := int32(0)
			proc := NewProc()
			ctx := context.WithValue(context.Background(), ContextCurrentProc, proc)
			randGen := rand.NewSource(int64(op))
			for j := 0; j < nOperation; j++ {
				mode := LWExclusive //LWLockMode(1 - (rand.Int()%10)/8)
				if rand.Float64() <= readPercentage {
					mode = LWShared
				}
				if locker.LWLockConditionAcquire(ctx, mode) {
					readSum := 0
					for k := 0; k < opLen; k++ {
						if mode == LWExclusive {
							*value += int(randGen.Int63() * (randGen.Int63()%3 - 1))
						} else {
							readSum += *value
						}
					}
					operationCount++
					locker.LWLockReleaseMode(ctx, mode)
				}
			}
			atomic.AddInt32(&totalCount, operationCount)
			//fmt.Printf("PROC %d done.\n", proc.ProcID)
			wg.Done()
		}(i, locker, &sharedValue)
	}

	wg.Wait()
	cost := time.Since(start).Seconds()
	fmt.Printf("Throughput is %.2f op/s\n", float64(totalCount)/cost)
}

func TestLWLockConditionalAcquire(t *testing.T) {
	// If we only consider a single lock for each transaction.
	// The abort of a lock acquire request's side effect is limited.
	// However, in real world case where Xacts are used, the abort will cause much more
	// resource waste.
	concurrent, batch, instructs := 32, 100, 1000
	c := &LWLock{}
	c.Init(0)
	benchmarkLWLockAcquireConditional(c, concurrent, batch, instructs, 0)
	benchmarkLWLockAcquireConditional(c, concurrent, batch, instructs, 0.5)
	benchmarkLWLockAcquireConditional(c, concurrent, batch, instructs, 1)
}

func TestLWLockConflictsWithVar(t *testing.T) {
	c := &LWLock{}
	c.Init(0)

	proc := NewProc()
	ctx := context.WithValue(context.Background(), ContextCurrentProc, proc)
	protectedValue := uint64(0)
	readSnapshot := protectedValue

	// the waitForUpdate shall continue when no one is updating the read snapshot.
	cur, lockFree, needWait := c.LWLockConflictsWithVar(ctx, readSnapshot, &protectedValue)
	assert.Equal(t, readSnapshot, cur)
	assert.True(t, lockFree)
	assert.False(t, needWait)

	// the waitForUpdate shall block when someone is updating the read snapshot.
	assert.False(t, c.LWLockAttemptLock(LWExclusive))
	cur, lockFree, needWait = c.LWLockConflictsWithVar(ctx, readSnapshot, &protectedValue)
	assert.Equal(t, readSnapshot, cur)
	assert.False(t, lockFree)
	assert.True(t, needWait)

	protectedValue = 1
	// The value has been updated!!!
	cur, lockFree, needWait = c.LWLockConflictsWithVar(ctx, readSnapshot, &protectedValue)
	assert.Equal(t, protectedValue, cur)
	assert.False(t, lockFree)
	assert.False(t, needWait)
	c.lockReleaseNoWakeUp(LWExclusive)
}

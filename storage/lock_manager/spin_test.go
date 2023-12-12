package lock_manager

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func benchmark(locker *SpinLock, nRoutine int, nOperation int) {
	wg := sync.WaitGroup{}
	start := time.Now()
	sharedValue := 0
	for i := 0; i < nRoutine; i++ {
		wg.Add(1)
		go func(op int, lock *SpinLock, value *int) {
			//s := time.Now()
			for j := 0; j < nOperation; j++ {
				locker.Lock()
				*value = op
				locker.Unlock()
			}

			//fmt.Printf("the maximum lock latency on go routine %d is %v ns/op\n",
			//	op, float64(time.Since(s).Nanoseconds())/float64(nOperation))
			wg.Done()
		}(i, locker, &sharedValue)
	}

	wg.Wait()
	cost := time.Since(start).Seconds()
	fmt.Printf("Throughput is %.2f op/s\n", float64(nRoutine)*float64(nOperation)/cost)
}

// Benchmark result:
// 1 routine:
//Throughput is 75061550.47 op/s
//Throughput is 73676222.47 op/s
//Throughput is 78658402.29 op/s
//Throughput is 26402082.60 op/s
//
// 10 routine:
//Throughput is 3676641.71 op/s
//Throughput is 40516570.06 op/s
//Throughput is 18902551.86 op/s
//Throughput is 7157768.31 op/s
//
// 100 routine:
//Throughput is 315290.29 op/s
//Throughput is 34539842.26 op/s
//Throughput is 13457773.42 op/s
//Throughput is 6735421.85 op/s

func TestSpinLock(t *testing.T) {
	r, o := 100, 10000
	var lock *SpinLock
	lock = NewSpinLockWithType(BusyLoop)
	benchmark(lock, r, o)
	lock = NewSpinLockWithType(NoBusyLoop)
	benchmark(lock, r, o)
	lock = NewSpinLockWithType(Native)
	benchmark(lock, r, o)
	lock = NewSpinLockWithType(ChannelBased)
	benchmark(lock, r, o)
}

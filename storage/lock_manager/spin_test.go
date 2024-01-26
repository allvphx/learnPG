package lock_manager

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func benchmarkSpinLock(locker *SpinLock, nRoutine, nOperation, opLen int) {
	wg := sync.WaitGroup{}
	start := time.Now()
	sharedValue := 0
	for i := 0; i < nRoutine; i++ {
		wg.Add(1)
		go func(op int, lock *SpinLock, value *int) {
			//s := time.Now()
			randGen := rand.NewSource(int64(op))
			for j := 0; j < nOperation; j++ {
				locker.Lock()
				for k := 0; k < opLen; k++ {
					*value += int(randGen.Int63() * (randGen.Int63()%3 - 1))
				}
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
// 1 routine, 1 RMW: the channel based lock has higher overhead.
//Throughput is 53304051.64 op/s
//Throughput is 54491161.53 op/s
//Throughput is 57990849.04 op/s
//Throughput is 25324918.71 op/s
// 100 routine, 1 RMW: the spin lock is suitable for short instructions.
//Throughput is 241750.23 op/s
//Throughput is 21625016.63 op/s
//Throughput is 14612141.60 op/s
//Throughput is 5840466.52 op/s
// 100 routine, 10 RMW: as instruction length goes high, the Golang native lock is the best.
//Throughput is 190358.77 op/s
//Throughput is 5872760.13 op/s
//Throughput is 10242722.93 op/s
//Throughput is 4616589.37 op/s

// The PG Contented lock seems to be beaten by spin lock (short instructions) or native lock (long instructions).
func TestSpinLock(t *testing.T) {
	concurrent, batch, instructs := 32, 100, 1000
	var lock *SpinLock
	lock = NewSpinLockWithType(BusyLoop)
	benchmarkSpinLock(lock, concurrent, batch, instructs)
	lock = NewSpinLockWithType(NoBusyLoop)
	benchmarkSpinLock(lock, concurrent, batch, instructs)
	lock = NewSpinLockWithType(Native)
	benchmarkSpinLock(lock, concurrent, batch, instructs)
	lock = NewSpinLockWithType(ChannelBased)
	benchmarkSpinLock(lock, concurrent, batch, instructs)
	lock = NewSpinLockWithType(PGContentedLock)
	benchmarkSpinLock(lock, concurrent, batch, instructs)
}

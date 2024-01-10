package port

import (
	"runtime"
	"sync/atomic"
)

func SpinStyleAtomicFetchAndModifyUint32(s *atomic.Uint32, f func(x uint32) uint32) uint32 {
	for {
		oldS := s.Load()
		newS := f(oldS)
		if newS == oldS || s.CompareAndSwap(oldS, newS) {
			return oldS
		}
		runtime.Gosched()
	}
}

func AtomicFetchOrUint32(s *atomic.Uint32, or uint32) uint32 {
	return SpinStyleAtomicFetchAndModifyUint32(s, func(x uint32) uint32 {
		return x | or
	})
}

func AtomicFetchAndUint32(s *atomic.Uint32, and uint32) uint32 {
	return SpinStyleAtomicFetchAndModifyUint32(s, func(x uint32) uint32 {
		return x & and
	})
}

func AtomicFetchSubUint32(s *atomic.Uint32, sub uint32) uint32 {
	return SpinStyleAtomicFetchAndModifyUint32(s, func(x uint32) uint32 {
		return x - sub
	})
}

var barrierTempValue = uint32(0)

func WriteBarrier() {
	atomic.AddUint32(&barrierTempValue, 1)
}

func ReadBarrier() {
	atomic.AddUint32(&barrierTempValue, 1)
}

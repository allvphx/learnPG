package err

import (
	"fmt"
	"runtime"
)

func Assert(cond bool, msg string) {
	if !cond {
		_, file, line, ok := runtime.Caller(1) // Get info about caller
		if ok {
			fmt.Printf("Assertion failed at %s:%d: %s\n", file, line, msg)
		} else {
			fmt.Printf("Assertion failed: %s\n", msg)
		}
		panic(msg)
	}
}

func SoftAssert(cond bool, msg string) {
	if !cond {
		_, file, line, ok := runtime.Caller(1) // Get info about caller
		if ok {
			fmt.Printf("Assertion failed at %s:%d: %s\n", file, line, msg)
		} else {
			fmt.Printf("Assertion failed: %s\n", msg)
		}
	}
}

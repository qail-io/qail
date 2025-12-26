// Micro-benchmark: CString allocation overhead
//
// Tests if CString creation is a bottleneck

package main

import (
	"fmt"
	"time"
	"unsafe"
)

/*
#cgo LDFLAGS: -L../../target/release -lqail_go -lresolv -framework Security -framework CoreFoundation
#include <stdlib.h>
#include <stdint.h>

// Just for timing CString allocation
*/
import "C"

const ITERATIONS = 1000000

func main() {
	fmt.Println("🔍 CSTRING OVERHEAD ANALYSIS")
	fmt.Println("============================")
	fmt.Printf("Iterations: %d\n\n", ITERATIONS)

	// Test 1: CString allocation only
	fmt.Println("📊 Test 1: CString allocation + free")
	start := time.Now()
	for i := 0; i < ITERATIONS; i++ {
		cStr := C.CString("harbors")
		C.free(unsafe.Pointer(cStr))
	}
	elapsed := time.Since(start)
	perOp := float64(elapsed.Nanoseconds()) / float64(ITERATIONS)
	fmt.Printf("   Per CString: %.0fns\n\n", perOp)

	// Test 2: Multiple CString allocations (like we do per batch)
	fmt.Println("📊 Test 2: 2x CString (table + columns)")
	start = time.Now()
	for i := 0; i < ITERATIONS; i++ {
		cTable := C.CString("harbors")
		cCols := C.CString("id,name")
		C.free(unsafe.Pointer(cTable))
		C.free(unsafe.Pointer(cCols))
	}
	elapsed = time.Since(start)
	perOp = float64(elapsed.Nanoseconds()) / float64(ITERATIONS)
	fmt.Printf("   Per 2x CString: %.0fns\n\n", perOp)

	// Test 3: Empty loop (baseline)
	fmt.Println("📊 Test 3: Empty loop (baseline)")
	start = time.Now()
	sum := 0
	for i := 0; i < ITERATIONS; i++ {
		sum += i
	}
	elapsed = time.Since(start)
	perOp = float64(elapsed.Nanoseconds()) / float64(ITERATIONS)
	fmt.Printf("   Per empty iteration: %.2fns (sum=%d)\n\n", perOp, sum%10)

	// Summary
	fmt.Println("📈 CONCLUSION:")
	fmt.Println("If CString is >1000ns, it's a major bottleneck")
	fmt.Println("We do 2 CString per batch (not per query), so this shouldn't be huge")
}

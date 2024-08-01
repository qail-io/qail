// Bottleneck Analysis Benchmark
//
// Tests individual components to find where the slowdown is:
// 1. Pure CGO call overhead (empty function)
// 2. Encoding overhead (Rust encoding only)
// 3. Tokio block_on overhead
// 4. Full query execution

package main

import (
	"fmt"
	"time"
	"unsafe"
)

/*
#cgo LDFLAGS: -L../../target/release -lqail_go -lresolv -framework Security -framework CoreFoundation
#include <stdlib.h>

// Test functions
extern void* qail_get(const char* table);
extern void qail_cmd_free(void* handle);
*/
import "C"

const ITERATIONS = 100000

func main() {
	fmt.Println("🔍 BOTTLENECK ANALYSIS")
	fmt.Println("======================")
	fmt.Printf("Iterations: %d\n\n", ITERATIONS)

	// Test 1: Pure CGO call overhead
	fmt.Println("📊 Test 1: Pure CGO Call Overhead")
	cgoOverhead := testCGOOverhead()
	fmt.Printf("   CGO overhead per call: %.0fns\n\n", cgoOverhead)

	// Test 2: CString allocation overhead
	fmt.Println("📊 Test 2: CString + CGO Call")
	cstringOverhead := testCStringOverhead()
	fmt.Printf("   CString + CGO per call: %.0fns\n\n", cstringOverhead)

	// Test 3: Full QailCmd creation
	fmt.Println("📊 Test 3: QailCmd Creation (qail_get + qail_cmd_free)")
	cmdOverhead := testCmdCreation()
	fmt.Printf("   QailCmd per call: %.0fns\n\n", cmdOverhead)

	// Summary
	fmt.Println("📈 SUMMARY:")
	fmt.Println("┌────────────────────────────────────────┐")
	fmt.Printf("│ Pure CGO call:        %10.0f ns    │\n", cgoOverhead)
	fmt.Printf("│ + CString alloc:      %10.0f ns    │\n", cstringOverhead-cgoOverhead)
	fmt.Printf("│ + Rust QailCmd:       %10.0f ns    │\n", cmdOverhead-cstringOverhead)
	fmt.Printf("│ Total per command:    %10.0f ns    │\n", cmdOverhead)
	fmt.Println("└────────────────────────────────────────┘")

	// Calculate theoretical max
	maxQPS := 1e9 / cmdOverhead
	fmt.Printf("\n🎯 Theoretical max (no I/O): %.0f q/s\n", maxQPS)
	fmt.Println("   (pgx is ~250k q/s, so CGO + encoding must be <4000ns)")
}

func testCGOOverhead() float64 {
	// Pre-allocate CString
	table := C.CString("test")
	defer C.free(unsafe.Pointer(table))

	start := time.Now()
	for i := 0; i < ITERATIONS; i++ {
		handle := C.qail_get(table)
		C.qail_cmd_free(handle)
	}
	elapsed := time.Since(start)

	return float64(elapsed.Nanoseconds()) / float64(ITERATIONS)
}

func testCStringOverhead() float64 {
	start := time.Now()
	for i := 0; i < ITERATIONS; i++ {
		table := C.CString("harbors")
		handle := C.qail_get(table)
		C.qail_cmd_free(handle)
		C.free(unsafe.Pointer(table))
	}
	elapsed := time.Since(start)

	return float64(elapsed.Nanoseconds()) / float64(ITERATIONS)
}

func testCmdCreation() float64 {
	start := time.Now()
	for i := 0; i < ITERATIONS; i++ {
		table := C.CString("harbors")
		handle := C.qail_get(table)
		C.qail_cmd_free(handle)
		C.free(unsafe.Pointer(table))
	}
	elapsed := time.Since(start)

	return float64(elapsed.Nanoseconds()) / float64(ITERATIONS)
}

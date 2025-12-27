// Package main - 50M query benchmark using PREPARED STATEMENT mode
// Should match native Rust performance (~355K q/s)
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/qail-lang/qail-go/ipc"
)

const (
	iterations = 50_000_000  // 50 million (fair comparison with pgx)
	batchSize  = 10_000      // 10K per pipeline batch (same as pgx)
)

func main() {
	fmt.Println("🚀 QAIL Go IPC - Prepared Statement Benchmark")
	fmt.Println("==============================================")
	fmt.Printf("Total queries: %d\n", iterations)
	fmt.Printf("Batch size:    %d\n", batchSize)
	fmt.Println()

	client, err := ipc.Connect("")
	if err != nil {
		fmt.Printf("❌ Failed to connect to daemon: %v\n", err)
		fmt.Println("   Start daemon: ./target/release/qail-daemon")
		os.Exit(1)
	}
	defer client.Close()

	// Connect to database
	if err := client.ConnectPG("localhost", 5432, "orion", "swb_staging_local", ""); err != nil {
		fmt.Printf("❌ Failed to connect to database: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("✅ Connected to PostgreSQL")

	// PREPARE STATEMENT ONCE (like native Rust)
	sql := "SELECT id, name FROM harbors LIMIT $1"
	handle, err := client.Prepare(sql)
	if err != nil {
		fmt.Printf("❌ Failed to prepare statement: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("✅ Prepared statement: %s\n", handle)

	// Pre-build params batch (like native Rust - reuse same params)
	paramsBatch := make([][]string, batchSize)
	for i := range paramsBatch {
		limit := fmt.Sprintf("%d", (i%10)+1)
		paramsBatch[i] = []string{limit}
	}

	// Warmup
	fmt.Println("🔥 Warming up...")
	for i := 0; i < 10; i++ {
		_, _ = client.PreparedPipeline(handle, paramsBatch)
	}

	numBatches := iterations / batchSize
	fmt.Printf("📊 Running %d batches of %d queries...\n", numBatches, batchSize)

	start := time.Now()
	successfulQueries := 0

	for b := 0; b < numBatches; b++ {
		count, err := client.PreparedPipeline(handle, paramsBatch)
		if err != nil {
			fmt.Printf("❌ PreparedPipeline failed at batch %d: %v\n", b, err)
			os.Exit(1)
		}
		successfulQueries += count

		// Progress every 10%
		if b > 0 && b%(numBatches/10) == 0 {
			pct := (b * 100) / numBatches
			elapsed := time.Since(start)
			qps := float64(successfulQueries) / elapsed.Seconds()
			fmt.Printf("   %d%% complete - %.0f q/s\n", pct, qps)
		}
	}

	elapsed := time.Since(start)
	qps := float64(successfulQueries) / elapsed.Seconds()

	fmt.Println()
	fmt.Println("📈 Results (Prepared Statement Mode):")
	fmt.Printf("   Total time:     %.2f seconds\n", elapsed.Seconds())
	fmt.Printf("   Queries/sec:    %.0f\n", qps)
	fmt.Printf("   Native Rust:    355,000 q/s (baseline)\n")
	fmt.Printf("   Efficiency:     %.1f%% of native Rust\n", (qps/355000)*100)
	fmt.Printf("   vs PipelineFast: %.1fx improvement\n", qps/42000)
}

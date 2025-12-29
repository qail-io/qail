// Package main - 50M query benchmark for QAIL IPC pipeline
// Comparable to native Rust benchmark (355K q/s)
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/qail-lang/qail-go/ipc"
)

const (
	iterations = 1_000_000   // 1 million for quick results
	batchSize  = 1_000       // Smaller batches for IPC efficiency
)

func main() {
	fmt.Println("🚀 QAIL Go IPC - 50 Million Query Benchmark")
	fmt.Println("============================================")
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

	// Warmup
	fmt.Println("🔥 Warming up...")
	for i := 0; i < 10; i++ {
		batch := make([]ipc.Query, 1000)
		for j := range batch {
			batch[j] = ipc.Query{
				Table:   "harbors",
				Columns: []string{"id", "name"},
				Limit:   5,
			}
		}
		_, _ = client.Pipeline(batch)
	}

	// Pre-create batch template
	batchTemplate := make([]ipc.Query, batchSize)
	for i := range batchTemplate {
		batchTemplate[i] = ipc.Query{
			Table:   "harbors",
			Columns: []string{"id", "name"},
			Limit:   5,
		}
	}

	numBatches := iterations / batchSize
	fmt.Printf("📊 Running %d batches of %d queries...\n", numBatches, batchSize)

	start := time.Now()
	
	for b := 0; b < numBatches; b++ {
		_, err := client.PipelineFast(batchTemplate)
		if err != nil {
			fmt.Printf("❌ Pipeline failed at batch %d: %v\n", b, err)
			os.Exit(1)
		}
		
		// Progress every 10%
		if b > 0 && b%(numBatches/10) == 0 {
			pct := (b * 100) / numBatches
			elapsed := time.Since(start)
			qps := float64(b*batchSize) / elapsed.Seconds()
			fmt.Printf("   %d%% complete - %.0f q/s\n", pct, qps)
		}
	}

	elapsed := time.Since(start)
	qps := float64(iterations) / elapsed.Seconds()

	fmt.Println()
	fmt.Println("📈 Results:")
	fmt.Printf("   Total time:     %.2f seconds\n", elapsed.Seconds())
	fmt.Printf("   Queries/sec:    %.0f\n", qps)
	fmt.Printf("   Native Rust:    355,000 q/s (baseline)\n")
	fmt.Printf("   Efficiency:     %.1f%% of native Rust\n", (qps/355000)*100)
}

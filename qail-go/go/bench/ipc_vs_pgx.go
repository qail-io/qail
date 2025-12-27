// Package main benchmarks QAIL Go IPC vs pgx
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/qail-lang/qail-go/ipc"
)

const (
	dsn        = "postgres://orion@localhost:5432/swb_staging_local"
	iterations = 50_000_000  // 50 million for comparison with native Rust
	warmup     = 1000
)

func main() {
	fmt.Println("🏁 QAIL Go IPC vs pgx Benchmark")
	fmt.Println("================================")

	// Benchmark pgx
	pgxSpeed, err := benchmarkPgx()
	if err != nil {
		fmt.Printf("❌ pgx failed: %v\n", err)
		os.Exit(1)
	}

	// Benchmark QAIL IPC
	ipcSpeed, err := benchmarkQailIPC()
	if err != nil {
		fmt.Printf("❌ QAIL IPC failed: %v\n", err)
		fmt.Println("   Make sure qail-daemon is running: cargo run -p qail-daemon --release")
		os.Exit(1)
	}

	// Results
	fmt.Println("\n📊 Results:")
	fmt.Printf("   pgx:      %d queries/sec\n", pgxSpeed)
	fmt.Printf("   QAIL IPC: %d queries/sec\n", ipcSpeed)
	
	if ipcSpeed > pgxSpeed {
		speedup := float64(ipcSpeed) / float64(pgxSpeed)
		fmt.Printf("\n🔥 QAIL IPC is %.2fx faster than pgx!\n", speedup)
	} else {
		speedup := float64(pgxSpeed) / float64(ipcSpeed)
		fmt.Printf("\n📉 pgx is %.2fx faster than QAIL IPC\n", speedup)
	}
}

func benchmarkPgx() (int, error) {
	ctx := context.Background()
	
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return 0, fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)

	// Warmup
	for i := 0; i < warmup; i++ {
		rows, err := conn.Query(ctx, "SELECT id, name FROM harbors LIMIT 5")
		if err != nil {
			return 0, fmt.Errorf("warmup query: %w", err)
		}
		for rows.Next() {
			var id int
			var name string
			_ = rows.Scan(&id, &name)
		}
		rows.Close()
	}

	// Benchmark
	start := time.Now()
	for i := 0; i < iterations; i++ {
		rows, err := conn.Query(ctx, "SELECT id, name FROM harbors LIMIT 5")
		if err != nil {
			return 0, fmt.Errorf("query: %w", err)
		}
		for rows.Next() {
			var id int
			var name string
			_ = rows.Scan(&id, &name)
		}
		rows.Close()
	}
	elapsed := time.Since(start)

	return int(float64(iterations) / elapsed.Seconds()), nil
}

func benchmarkQailIPC() (int, error) {
	client, err := ipc.Connect("")
	if err != nil {
		return 0, fmt.Errorf("connect socket: %w", err)
	}
	defer client.Close()

	// Connect to database via IPC
	if err := client.ConnectPG("localhost", 5432, "orion", "swb_staging_local", ""); err != nil {
		return 0, fmt.Errorf("connect db: %w", err)
	}

	// Warmup with single queries
	for i := 0; i < warmup; i++ {
		_, err := client.Get("harbors", []string{"id", "name"}, 5)
		if err != nil {
			return 0, fmt.Errorf("warmup: %w", err)
		}
	}

	// Benchmark using TRUE PostgreSQL pipeline mode with larger batches
	batchSize := 5000  // Increased from 1000
	numBatches := iterations / batchSize
	
	start := time.Now()
	for b := 0; b < numBatches; b++ {
		batch := make([]ipc.Query, batchSize)
		for i := range batch {
			batch[i] = ipc.Query{
				Table:   "harbors",
				Columns: []string{"id", "name"},
				Limit:   5,
			}
		}
		// Use Pipeline instead of QueryBatch for true async
		_, err := client.Pipeline(batch)
		if err != nil {
			return 0, fmt.Errorf("pipeline %d: %w", b, err)
		}
	}
	elapsed := time.Since(start)

	return int(float64(iterations) / elapsed.Seconds()), nil
}

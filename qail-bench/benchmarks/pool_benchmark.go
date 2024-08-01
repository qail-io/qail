// Go pgx POOL BENCHMARK (BATCHED)
//
// Tests connection pool performance with concurrent workers.
// Uses pgx.Batch to be fair with QAIL pipelining.
//
// Run: go run pool_benchmark.go

package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	TOTAL_QUERIES     = 150_000_000
	NUM_WORKERS       = 10
	POOL_SIZE         = 10
	QUERIES_PER_BATCH = 100
)

func getEnvOr(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func main() {
	ctx := context.Background()

	host := getEnvOr("PG_HOST", "127.0.0.1")
	port := getEnvOr("PG_PORT", "5432")
	user := getEnvOr("PG_USER", "postgres")
	database := getEnvOr("PG_DATABASE", "postgres")

	connStr := fmt.Sprintf("postgres://%s@%s:%s/%s?pool_max_conns=%d&pool_min_conns=%d",
		user, host, port, database, POOL_SIZE, POOL_SIZE)

	fmt.Printf("🔌 Connecting to %s:%s as %s\n", host, port, user)

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	fmt.Println("🚀 GO PGX POOL BENCHMARK (BATCHED)")
	fmt.Println("==================================")
	fmt.Printf("Total queries:    %15d\n", TOTAL_QUERIES)
	fmt.Printf("Workers:          %15d\n", NUM_WORKERS)
	fmt.Printf("Pool size:        %15d\n", POOL_SIZE)
	fmt.Printf("Batch size:       %15d\n", QUERIES_PER_BATCH)
	fmt.Println()

	batchesPerWorker := TOTAL_QUERIES / NUM_WORKERS / QUERIES_PER_BATCH
	var counter int64
	var wg sync.WaitGroup

	start := time.Now()

	// Spawn workers
	for w := 0; w < NUM_WORKERS; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			for b := 0; b < batchesPerWorker; b++ {
				// Acquire connection
				conn, err := pool.Acquire(ctx)
				if err != nil {
					fmt.Printf("Worker %d: acquire failed: %v\n", workerID, err)
					return
				}

				// Build Batch
				batch := &pgx.Batch{}
				for i := 0; i < QUERIES_PER_BATCH; i++ {
					limit := (i % 10) + 1
					batch.Queue("SELECT id, name FROM harbors LIMIT $1", limit)
				}

				// Send Batch
				br := conn.SendBatch(ctx, batch)
				
				// Consume Results
				for i := 0; i < QUERIES_PER_BATCH; i++ {
					rows, err := br.Query()
					if err != nil {
						fmt.Printf("Worker %d: batch query failed: %v\n", workerID, err)
						break
					}
					// Consume rows
					for rows.Next() {
						var id string
						var name string
						rows.Scan(&id, &name)
					}
					rows.Close()
				}
				
				br.Close()
				conn.Release()
				atomic.AddInt64(&counter, int64(QUERIES_PER_BATCH))
			}
		}(w)
	}

	// Progress reporter
	done := make(chan bool)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(2 * time.Second):
				count := atomic.LoadInt64(&counter)
				if count >= TOTAL_QUERIES {
					return
				}
				elapsed := time.Since(start).Seconds()
				qps := float64(count) / elapsed
				remaining := TOTAL_QUERIES - int(count)
				eta := float64(remaining) / qps
				fmt.Printf("   %6d queries | %8.0f q/s | ETA: %.0fs\n", count, qps, eta)
			}
		}
	}()

	wg.Wait()
	done <- true

	elapsed := time.Since(start)
	qps := float64(TOTAL_QUERIES) / elapsed.Seconds()

	fmt.Println("\n📈 FINAL RESULTS:")
	fmt.Println("┌──────────────────────────────────────────────────┐")
	fmt.Println("│ GO PGX POOL BENCHMARK (BATCHED)                  │")
	fmt.Println("├──────────────────────────────────────────────────┤")
	fmt.Printf("│ Total Time:               %15.1fs │\n", elapsed.Seconds())
	fmt.Printf("│ Queries/Second:           %15.0f │\n", qps)
	fmt.Printf("│ Workers:                  %15d │\n", NUM_WORKERS)
	fmt.Printf("│ Pool Size:                %15d │\n", POOL_SIZE)
	fmt.Printf("│ Queries Completed:        %15d │\n", atomic.LoadInt64(&counter))
	fmt.Println("└──────────────────────────────────────────────────┘")
}

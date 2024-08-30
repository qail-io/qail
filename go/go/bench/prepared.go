// V3: Prepared Batch Benchmark - ZERO CGO in hot path!
//
// Encodes query ONCE via CGO, then executes via pure Go I/O
//
// Run:
//   cd qail-go/go && go run bench/prepared.go

package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	qail "github.com/qail-lang/qail-go"
)

const (
	QUERIES_PER_BATCH = 1000
	BATCHES           = 100
	TOTAL_QUERIES     = QUERIES_PER_BATCH * BATCHES
)

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	host := getEnv("PG_HOST", "127.0.0.1")
	port := getEnv("PG_PORT", "5432")
	user := getEnv("PG_USER", "postgres")
	database := getEnv("PG_DATABASE", "postgres")

	fmt.Println("🏁 V3 PREPARED BATCH BENCHMARK")
	fmt.Println("==============================")
	fmt.Printf("Total queries: %d\n", TOTAL_QUERIES)
	fmt.Printf("Batch size:    %d\n", QUERIES_PER_BATCH)
	fmt.Printf("Batches:       %d\n\n", BATCHES)

	// ========== PGX BENCHMARK ==========
	fmt.Println("📊 [1/2] PGX (Pure Go)...")
	pgxQPS := benchmarkPGX(host, port, user, database)

	// ========== QAIL PREPARED BENCHMARK ==========
	fmt.Println("\n📊 [2/2] QAIL-GO V3 (Prepared Batch)...")
	qailQPS := benchmarkQAILPrepared(host, port, user, database)

	// ========== RESULTS ==========
	fmt.Println("\n📈 RESULTS:")
	fmt.Println("┌──────────────────────────────────────────┐")
	fmt.Println("│ V3 PREPARED BATCH vs PGX                 │")
	fmt.Println("├──────────────────────────────────────────┤")
	fmt.Printf("│ PGX:             %23.0f q/s │\n", pgxQPS)
	fmt.Printf("│ QAIL Prepared:   %23.0f q/s │\n", qailQPS)
	fmt.Println("├──────────────────────────────────────────┤")

	if qailQPS > pgxQPS {
		speedup := qailQPS / pgxQPS
		fmt.Printf("│ 🏆 QAIL is %.2fx FASTER than pgx!       │\n", speedup)
	} else {
		ratio := qailQPS / pgxQPS * 100
		fmt.Printf("│ QAIL is %.1f%% of PGX speed              │\n", ratio)
	}
	fmt.Println("└──────────────────────────────────────────┘")
}

func benchmarkPGX(host, port, user, database string) float64 {
	ctx := context.Background()
	connStr := fmt.Sprintf("postgres://%s@%s:%s/%s?sslmode=disable", user, host, port, database)

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		fmt.Printf("PGX connect error: %v\n", err)
		return 0
	}
	defer conn.Close(ctx)

	// Pre-allocate limits
	limits := make([]int, QUERIES_PER_BATCH)
	for i := 0; i < QUERIES_PER_BATCH; i++ {
		limits[i] = (i % 10) + 1
	}

	// Warmup
	conn.Exec(ctx, "SELECT 1")

	start := time.Now()
	successfulQueries := 0

	for batch := 0; batch < BATCHES; batch++ {
		b := &pgx.Batch{}
		for i := 0; i < QUERIES_PER_BATCH; i++ {
			b.Queue("SELECT id, name FROM harbors LIMIT $1", limits[i])
		}

		br := conn.SendBatch(ctx, b)
		for i := 0; i < QUERIES_PER_BATCH; i++ {
			_, err := br.Exec()
			if err != nil {
				fmt.Printf("PGX batch error: %v\n", err)
				br.Close()
				return 0
			}
			successfulQueries++
		}
		br.Close()

		if batch%20 == 0 {
			fmt.Printf("   Batch %d/%d\n", batch, BATCHES)
		}
	}

	elapsed := time.Since(start)
	qps := float64(successfulQueries) / elapsed.Seconds()
	fmt.Printf("   Completed: %d queries in %.2fs\n", successfulQueries, elapsed.Seconds())
	return qps
}

func benchmarkQAILPrepared(host, port, user, database string) float64 {
	driver, err := qail.NewDriver(qail.Config{
		Host:     host,
		Port:     port,
		User:     user,
		Database: database,
		SSLMode:  "disable",
		PoolSize: 1,
	})
	if err != nil {
		fmt.Printf("QAIL connect error: %v\n", err)
		return 0
	}
	defer driver.Close()

	// PREPARE ONCE - ONE CGO call!
	prepared := driver.PrepareBatchN("harbors", "id,name", QUERIES_PER_BATCH)
	if prepared == nil {
		fmt.Println("Failed to prepare batch")
		return 0
	}
	fmt.Println("   ✅ Prepared batch (ONE CGO call done)")

	start := time.Now()
	successfulQueries := 0

	for batch := 0; batch < BATCHES; batch++ {
		// Execute with ZERO CGO - pure Go I/O!
		completed, err := driver.ExecutePrepared(prepared)
		if err != nil {
			fmt.Printf("QAIL batch error: %v\n", err)
			return 0
		}
		successfulQueries += completed

		if batch%20 == 0 {
			fmt.Printf("   Batch %d/%d\n", batch, BATCHES)
		}
	}

	elapsed := time.Since(start)
	qps := float64(successfulQueries) / elapsed.Seconds()
	fmt.Printf("   Completed: %d queries in %.2fs\n", successfulQueries, elapsed.Seconds())
	return qps
}

// QAIL-Go vs PGX Benchmark
//
// Run:
//   cd qail-go/go && go run bench/qail_vs_pgx.go
//
// Requires:
//   - PostgreSQL with 'harbors' table
//   - Environment: PG_HOST, PG_PORT, PG_USER, PG_DATABASE

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

	fmt.Println("🏁 QAIL-GO vs PGX BENCHMARK")
	fmt.Println("===========================")
	fmt.Printf("Total queries: %d\n", TOTAL_QUERIES)
	fmt.Printf("Batch size:    %d\n", QUERIES_PER_BATCH)
	fmt.Printf("Batches:       %d\n\n", BATCHES)

	// ========== PGX BENCHMARK ==========
	fmt.Println("📊 [1/2] PGX Benchmark...")
	pgxQPS := benchmarkPGX(host, port, user, database)

	// ========== QAIL-GO BENCHMARK ==========
	fmt.Println("\n📊 [2/2] QAIL-GO Benchmark...")
	qailQPS := benchmarkQAIL(host, port, user, database)

	// ========== RESULTS ==========
	fmt.Println("\n📈 RESULTS:")
	fmt.Println("┌──────────────────────────────────────────┐")
	fmt.Println("│ QAIL-GO vs PGX BENCHMARK                 │")
	fmt.Println("├──────────────────────────────────────────┤")
	fmt.Printf("│ PGX:      %30.0f q/s │\n", pgxQPS)
	fmt.Printf("│ QAIL-GO:  %30.0f q/s │\n", qailQPS)
	fmt.Println("├──────────────────────────────────────────┤")

	if qailQPS > pgxQPS {
		speedup := qailQPS / pgxQPS
		fmt.Printf("│ 🏆 QAIL-GO is %.1fx faster!              │\n", speedup)
	} else {
		ratio := qailQPS / pgxQPS * 100
		fmt.Printf("│ QAIL-GO is %.1f%% of PGX speed            │\n", ratio)
	}
	fmt.Println("└──────────────────────────────────────────┘")
}

func benchmarkPGX(host, port, user, database string) float64 {
	ctx := context.Background()
	password := os.Getenv("PG_PASSWORD")
	var connStr string
	if password != "" {
		connStr = fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, password, host, port, database)
	} else {
		connStr = fmt.Sprintf("postgres://%s@%s:%s/%s?sslmode=disable", user, host, port, database)
	}

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		fmt.Printf("PGX connect error: %v\n", err)
		return 0
	}
	defer conn.Close(ctx)

	// Warmup
	conn.Exec(ctx, "SELECT 1")

	start := time.Now()
	successfulQueries := 0

	for batch := 0; batch < BATCHES; batch++ {
		b := &pgx.Batch{}
		for i := 0; i < QUERIES_PER_BATCH; i++ {
			limit := (i % 10) + 1
			b.Queue("SELECT id, name FROM harbors LIMIT $1", limit)
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

func benchmarkQAIL(host, port, user, database string) float64 {
	password := os.Getenv("PG_PASSWORD")
	driver, err := qail.NewDriver(qail.Config{
		Host:     host,
		Port:     port,
		User:     user,
		Database: database,
		Password: password,
		SSLMode:  "disable",
		PoolSize: 1,
	})
	if err != nil {
		fmt.Printf("QAIL connect error: %v\n", err)
		return 0
	}
	defer driver.Close()

	start := time.Now()
	successfulQueries := 0

	// Pre-allocate limits array ONCE
	limits := make([]int64, QUERIES_PER_BATCH)
	for i := 0; i < QUERIES_PER_BATCH; i++ {
		limits[i] = int64((i % 10) + 1)
	}

	for batch := 0; batch < BATCHES; batch++ {
		// Execute batch with ONE CGO call!
		completed, err := driver.BatchExecuteFast("harbors", "id,name", limits)
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

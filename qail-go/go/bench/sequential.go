// QAIL-Go vs PGX Sequential Benchmark
//
// Tests individual queries (not batched) to measure per-query overhead
//
// Run:
//   cd qail-go/go && go run bench/sequential.go

package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	qail "github.com/qail-lang/qail-go"
)

const TOTAL_QUERIES = 10000

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

	fmt.Println("🏁 SEQUENTIAL QUERY BENCHMARK")
	fmt.Println("=============================")
	fmt.Printf("Total queries: %d\n\n", TOTAL_QUERIES)

	// ========== PGX BENCHMARK ==========
	fmt.Println("📊 [1/2] PGX Sequential...")
	pgxQPS := benchmarkPGXSeq(host, port, user, database)

	// ========== QAIL-GO BENCHMARK ==========
	fmt.Println("\n📊 [2/2] QAIL-GO Sequential...")
	qailQPS := benchmarkQAILSeq(host, port, user, database)

	// ========== RESULTS ==========
	fmt.Println("\n📈 RESULTS:")
	fmt.Println("┌──────────────────────────────────────────┐")
	fmt.Println("│ SEQUENTIAL QUERY BENCHMARK               │")
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

func benchmarkPGXSeq(host, port, user, database string) float64 {
	ctx := context.Background()
	connStr := fmt.Sprintf("postgres://%s@%s:%s/%s?sslmode=disable", user, host, port, database)

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

	for i := 0; i < TOTAL_QUERIES; i++ {
		limit := (i % 10) + 1
		_, err := conn.Exec(ctx, "SELECT id, name FROM harbors LIMIT $1", limit)
		if err != nil {
			fmt.Printf("PGX query error: %v\n", err)
			return 0
		}
		successfulQueries++

		if i%2000 == 0 {
			fmt.Printf("   Query %d/%d\n", i, TOTAL_QUERIES)
		}
	}

	elapsed := time.Since(start)
	qps := float64(successfulQueries) / elapsed.Seconds()
	fmt.Printf("   Completed: %d queries in %.2fs\n", successfulQueries, elapsed.Seconds())
	return qps
}

func benchmarkQAILSeq(host, port, user, database string) float64 {
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

	start := time.Now()
	successfulQueries := 0

	for i := 0; i < TOTAL_QUERIES; i++ {
		limit := int64((i % 10) + 1)
		cmd := qail.Get("harbors").
			Columns("id", "name").
			Limit(limit)

		err := driver.Execute(cmd)
		cmd.Free()

		if err != nil {
			fmt.Printf("QAIL query error: %v\n", err)
			return 0
		}
		successfulQueries++

		if i%2000 == 0 {
			fmt.Printf("   Query %d/%d\n", i, TOTAL_QUERIES)
		}
	}

	elapsed := time.Since(start)
	qps := float64(successfulQueries) / elapsed.Seconds()
	fmt.Printf("   Completed: %d queries in %.2fs\n", successfulQueries, elapsed.Seconds())
	return qps
}

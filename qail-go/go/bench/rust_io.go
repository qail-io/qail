// QAIL-Go Rust I/O Benchmark
//
// Tests all I/O happening in Rust Tokio - bypasses Go network layer
//
// Run:
//   cd qail-go/go && go run bench/rust_io.go

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

	fmt.Println("🏁 RUST I/O BENCHMARK")
	fmt.Println("====================")
	fmt.Printf("Total queries: %d\n", TOTAL_QUERIES)
	fmt.Printf("Batch size:    %d\n", QUERIES_PER_BATCH)
	fmt.Printf("Batches:       %d\n\n", BATCHES)

	// ========== PGX BENCHMARK ==========
	fmt.Println("📊 [1/2] PGX (Pure Go)...")
	pgxQPS := benchmarkPGX(host, port, user, database)

	// ========== QAIL RUST I/O BENCHMARK ==========
	fmt.Println("\n📊 [2/2] QAIL-GO (Rust I/O)...")
	qailQPS := benchmarkQAILRustIO(host, port, user, database)

	// ========== RESULTS ==========
	fmt.Println("\n📈 RESULTS:")
	fmt.Println("┌──────────────────────────────────────────┐")
	fmt.Println("│ RUST I/O vs PGX BENCHMARK                │")
	fmt.Println("├──────────────────────────────────────────┤")
	fmt.Printf("│ PGX:          %26.0f q/s │\n", pgxQPS)
	fmt.Printf("│ QAIL Rust I/O:%26.0f q/s │\n", qailQPS)
	fmt.Println("├──────────────────────────────────────────┤")

	if qailQPS > pgxQPS {
		speedup := qailQPS / pgxQPS
		fmt.Printf("│ 🏆 QAIL Rust I/O is %.1fx faster!        │\n", speedup)
	} else {
		ratio := qailQPS / pgxQPS * 100
		fmt.Printf("│ QAIL Rust I/O is %.1f%% of PGX speed     │\n", ratio)
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

func benchmarkQAILRustIO(host, port, user, database string) float64 {
	// Connect using RUST I/O
	portNum := uint16(5432)
	conn, err := qail.RustConnect(host, portNum, user, database)
	if err != nil {
		fmt.Printf("QAIL Rust connect error: %v\n", err)
		return 0
	}
	defer conn.Close()

	// Pre-allocate limits
	limits := make([]int64, QUERIES_PER_BATCH)
	for i := 0; i < QUERIES_PER_BATCH; i++ {
		limits[i] = int64((i % 10) + 1)
	}

	start := time.Now()
	successfulQueries := int64(0)

	for batch := 0; batch < BATCHES; batch++ {
		// ONE CGO call for: encode + TCP write + TCP read + parse
		completed, err := conn.ExecuteBatch("harbors", "id,name", limits)
		if err != nil {
			fmt.Printf("QAIL Rust batch error: %v\n", err)
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

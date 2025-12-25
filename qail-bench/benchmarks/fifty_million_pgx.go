// 50 MILLION QUERY STRESS TEST - Go pgx
//
// For fair comparison with QAIL's benchmark.
//
// ## Configuration
//
// Set environment variables:
//   export PG_HOST=127.0.0.1
//   export PG_PORT=5432
//   export PG_USER=postgres
//   export PG_DATABASE=postgres
//
// ## Run
//
//   go run fifty_million_pgx.go

package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	TOTAL_QUERIES     = 50_000_000
	QUERIES_PER_BATCH = 10_000
	BATCHES           = TOTAL_QUERIES / QUERIES_PER_BATCH
)

func getEnvOr(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func main() {
	ctx := context.Background()

	// Read connection info from environment
	host := getEnvOr("PG_HOST", "127.0.0.1")
	port := getEnvOr("PG_PORT", "5432")
	user := getEnvOr("PG_USER", "postgres")
	database := getEnvOr("PG_DATABASE", "postgres")

	connStr := fmt.Sprintf("postgres://%s@%s:%s/%s", user, host, port, database)
	fmt.Printf("🔌 Connecting to %s:%s as %s\n", host, port, user)

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		panic(err)
	}
	defer conn.Close(ctx)

	fmt.Println("🚀 50 MILLION QUERY STRESS TEST - Go pgx")
	fmt.Println("=========================================")
	fmt.Printf("Total queries:    %15d\n", TOTAL_QUERIES)
	fmt.Printf("Batch size:       %15d\n", QUERIES_PER_BATCH)
	fmt.Printf("Batches:          %15d\n", BATCHES)
	fmt.Println("\n⚠️  Testing Go memory stability...\n")

	// Prepare params batch (reused for all batches - FAIR: same as QAIL)
	params := make([]int, QUERIES_PER_BATCH)
	for i := 0; i < QUERIES_PER_BATCH; i++ {
		params[i] = (i % 10) + 1
	}
	fmt.Println("✅ Params pre-built (same as QAIL)")

	fmt.Println("\n📊 Executing 50 million queries...\n")

	start := time.Now()
	successfulQueries := 0
	lastReport := time.Now()

	for batch := 0; batch < BATCHES; batch++ {
		// Build batch
		b := &pgx.Batch{}
		for i := 0; i < QUERIES_PER_BATCH; i++ {
			b.Queue("SELECT id, name FROM harbors LIMIT $1", params[i])
		}

		// Execute batch
		br := conn.SendBatch(ctx, b)
		for i := 0; i < QUERIES_PER_BATCH; i++ {
			_, err := br.Exec()
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				return
			}
		}
		br.Close()

		successfulQueries += QUERIES_PER_BATCH

		// Progress report every 1 million queries
		if successfulQueries%1_000_000 == 0 || time.Since(lastReport) >= 5*time.Second {
			elapsed := time.Since(start)
			qps := float64(successfulQueries) / elapsed.Seconds()
			remaining := TOTAL_QUERIES - successfulQueries
			eta := float64(remaining) / qps

			fmt.Printf("   %3dM queries | %8.0f q/s | ETA: %.0fs | Batch %d/%d\n",
				successfulQueries/1_000_000,
				qps,
				eta,
				batch+1,
				BATCHES)
			lastReport = time.Now()
		}
	}

	elapsed := time.Since(start)
	qps := float64(TOTAL_QUERIES) / elapsed.Seconds()
	perQueryNs := elapsed.Nanoseconds() / int64(TOTAL_QUERIES)

	fmt.Println("\n📈 FINAL RESULTS:")
	fmt.Println("┌──────────────────────────────────────────┐")
	fmt.Println("│ 50 MILLION QUERY STRESS TEST (Go pgx)    │")
	fmt.Println("├──────────────────────────────────────────┤")
	fmt.Printf("│ Total Time:        %20.1fs │\n", elapsed.Seconds())
	fmt.Printf("│ Queries/Second:    %20.0f │\n", qps)
	fmt.Printf("│ Per Query:         %17dns │\n", perQueryNs)
	fmt.Printf("│ Successful:        %20d │\n", successfulQueries)
	fmt.Println("│ GC Pauses:         Check with GODEBUG    │")
	fmt.Println("└──────────────────────────────────────────┘")
}

// 10 MILLION QUERY BENCHMARK - Go pgx WITH RESULT CONSUMPTION
//
// This test actually reads and parses row data for fair comparison.
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
//   go run fifty_million_consume.go

package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	TOTAL_QUERIES     = 10_000_000
	QUERIES_PER_BATCH = 1_000
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

	fmt.Println("🚀 10 MILLION QUERY BENCHMARK - Go pgx WITH RESULT CONSUMPTION")
	fmt.Println("==============================================================")
	fmt.Printf("Total queries:    %15d\n", TOTAL_QUERIES)
	fmt.Printf("Batch size:       %15d\n", QUERIES_PER_BATCH)
	fmt.Printf("Batches:          %15d\n", BATCHES)
	fmt.Println("\n⚠️  This test READS and PARSES all row data!\n")

	// Prepare params batch (reused for all batches - FAIR: same as QAIL)
	params := make([]int, QUERIES_PER_BATCH)
	for i := 0; i < QUERIES_PER_BATCH; i++ {
		params[i] = (i % 10) + 1
	}
	fmt.Println("✅ Params pre-built (same as QAIL)")

	fmt.Printf("\n📊 Executing %d queries with result consumption...\n\n", TOTAL_QUERIES)

	start := time.Now()
	successfulQueries := 0
	totalRows := int64(0)
	lastReport := time.Now()

	for batch := 0; batch < BATCHES; batch++ {
		// Build batch
		b := &pgx.Batch{}
		for i := 0; i < QUERIES_PER_BATCH; i++ {
			b.Queue("SELECT id, name FROM harbors LIMIT $1", params[i])
		}

		// Execute batch
		br := conn.SendBatch(ctx, b)

		// CONSUME: Actually read row data
		for i := 0; i < QUERIES_PER_BATCH; i++ {
			rows, err := br.Query()
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				return
			}

			// Read all rows
			for rows.Next() {
				var id int64
				var name string
				if err := rows.Scan(&id, &name); err != nil {
					fmt.Printf("Scan error: %v\n", err)
					rows.Close()
					return
				}
				totalRows++
				// Use values to prevent optimization away
				_ = id
				_ = name
			}
			rows.Close()
			successfulQueries++
		}
		br.Close()

		// Progress report every 1 million queries
		if successfulQueries%1_000_000 == 0 || time.Since(lastReport) >= 5*time.Second {
			elapsed := time.Since(start)
			qps := float64(successfulQueries) / elapsed.Seconds()
			remaining := TOTAL_QUERIES - successfulQueries
			eta := float64(remaining) / qps

			fmt.Printf("   %3dM queries | %8.0f q/s | ETA: %.0fs | Rows: %d | Batch %d/%d\n",
				successfulQueries/1_000_000,
				qps,
				eta,
				totalRows,
				batch+1,
				BATCHES)
			lastReport = time.Now()
		}
	}

	elapsed := time.Since(start)
	qps := float64(TOTAL_QUERIES) / elapsed.Seconds()
	perQueryNs := elapsed.Nanoseconds() / int64(TOTAL_QUERIES)

	fmt.Println("\n📈 FINAL RESULTS:")
	fmt.Println("┌──────────────────────────────────────────────────┐")
	fmt.Println("│ 10 MILLION QUERY - Go pgx WITH CONSUMPTION       │")
	fmt.Println("├──────────────────────────────────────────────────┤")
	fmt.Printf("│ Total Time:            %20.1fs │\n", elapsed.Seconds())
	fmt.Printf("│ Queries/Second:        %20.0f │\n", qps)
	fmt.Printf("│ Per Query:             %17dns │\n", perQueryNs)
	fmt.Printf("│ Successful:            %20d │\n", successfulQueries)
	fmt.Printf("│ Rows Parsed:           %20d │\n", totalRows)
	fmt.Printf("│ Avg Rows/Query:        %20.1f │\n", float64(totalRows)/float64(successfulQueries))
	fmt.Println("│ GC Pauses:            Check with GODEBUG         │")
	fmt.Println("└──────────────────────────────────────────────────┘")
}

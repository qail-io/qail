// Go pgx Pipelining Benchmark
// Compare with QAIL-PG query_pipeline()
//
// Run: go run pgx_benchmark.go

package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	QueriesPerBatch = 1000
	Batches         = 1000
)

func main() {
	password := os.Getenv("STAGING_DB_PASSWORD")
	if password == "" {
		fmt.Println("Set STAGING_DB_PASSWORD")
		os.Exit(1)
	}

	connStr := fmt.Sprintf("postgres://user:%s@127.0.0.1:5444/example-staging?sslmode=disable", password)
	
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		fmt.Printf("Connect error: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)

	totalQueries := Batches * QueriesPerBatch

	fmt.Println("🚀 GO PGX MILLION QUERY BENCHMARK")
	fmt.Println("==================================")
	fmt.Printf("Total queries: %d\n", totalQueries)
	fmt.Printf("Batch size:    %d\n", QueriesPerBatch)
	fmt.Printf("Batches:       %d\n\n", Batches)

	// Warmup
	conn.Exec(ctx, "SELECT 1")

	// ===== PIPELINED QUERIES (using Batch) =====
	fmt.Println("📊 Running pipeline benchmark...")

	start := time.Now()
	successfulQueries := 0

	for batch := 0; batch < Batches; batch++ {
		if batch%100 == 0 {
			fmt.Printf("   Batch %d/%d\n", batch, Batches)
		}

		// Use pgx Batch for pipelining
		b := &pgx.Batch{}
		for i := 1; i <= QueriesPerBatch; i++ {
			limit := (i % 10) + 1
			b.Queue("SELECT id, name FROM harbors LIMIT $1", limit)
		}

		br := conn.SendBatch(ctx, b)
		
		for i := 0; i < QueriesPerBatch; i++ {
			_, err := br.Exec()
			if err != nil {
				fmt.Printf("Batch query error: %v\n", err)
				br.Close()
				os.Exit(1)
			}
			successfulQueries++
		}
		br.Close()
	}

	elapsed := time.Since(start)

	// Results
	qps := float64(totalQueries) / elapsed.Seconds()
	perQueryNs := elapsed.Nanoseconds() / int64(totalQueries)

	fmt.Println("\n📈 Results:")
	fmt.Println("┌──────────────────────────────────────────┐")
	fmt.Println("│ GO PGX - ONE MILLION QUERIES             │")
	fmt.Println("├──────────────────────────────────────────┤")
	fmt.Printf("│ Total Time:     %23s │\n", elapsed.Round(time.Millisecond))
	fmt.Printf("│ Queries/Second: %23.0f │\n", qps)
	fmt.Printf("│ Per Query:      %20dns │\n", perQueryNs)
	fmt.Printf("│ Successful:     %23d │\n", successfulQueries)
	fmt.Println("└──────────────────────────────────────────┘")

	// Compare to theoretical serial
	theoreticalSerialSecs := float64(totalQueries) * 0.037
	speedup := theoreticalSerialSecs / elapsed.Seconds()

	fmt.Printf("\n🏆 vs Serial (37ms/query):\n")
	fmt.Printf("   Serial estimate:  %.0f seconds (%.1f hours)\n", theoreticalSerialSecs, theoreticalSerialSecs/3600.0)
	fmt.Printf("   Pipeline actual:  %.1f seconds\n", elapsed.Seconds())
	fmt.Printf("   Speedup:          %.0fx faster!\n", speedup)
}

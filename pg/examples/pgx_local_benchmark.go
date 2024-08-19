// Go pgx Local Pipelining Benchmark
// Compare with QAIL-PG pipeline_ast() on LOCAL PostgreSQL
//
// Run: go run pgx_local_benchmark.go

package main

import (
	"context"
	"fmt"
	"os/user"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	QueriesPerBatch = 1000
	Batches         = 1000
)

func main() {
	// Get current username for local PostgreSQL connection
	currentUser, _ := user.Current()
	username := currentUser.Username
	
	// Local PostgreSQL connection (no password, trust auth)
	connStr := fmt.Sprintf("postgres://%s@127.0.0.1:5432/example_staging?sslmode=disable", username)
	
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		fmt.Printf("Connect error: %v\n", err)
		fmt.Println("Make sure PostgreSQL 18 is running locally!")
		return
	}
	defer conn.Close(ctx)

	totalQueries := Batches * QueriesPerBatch

	fmt.Println("🚀 GO PGX LOCAL MILLION QUERY BENCHMARK")
	fmt.Println("=======================================")
	fmt.Printf("Total queries: %d\n", totalQueries)
	fmt.Printf("Batch size:    %d\n", QueriesPerBatch)
	fmt.Printf("Batches:       %d\n", Batches)
	fmt.Println("\n⚠️  LOCAL PostgreSQL - NO NETWORK LATENCY!\n")

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
				return
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
	fmt.Println("│ GO PGX LOCAL - ONE MILLION QUERIES       │")
	fmt.Println("├──────────────────────────────────────────┤")
	fmt.Printf("│ Total Time:     %23s │\n", elapsed.Round(time.Millisecond))
	fmt.Printf("│ Queries/Second: %23.0f │\n", qps)
	fmt.Printf("│ Per Query:      %20dns │\n", perQueryNs)
	fmt.Printf("│ Successful:     %23d │\n", successfulQueries)
	fmt.Println("└──────────────────────────────────────────┘")

	// Compare to QAIL local
	qailLocalQps := 73713.0
	qailLocalTime := 13.57
	
	fmt.Printf("\n📊 vs QAIL AST-native local (%.2fs @ %.0f q/s):\n", qailLocalTime, qailLocalQps)
	if qps > qailLocalQps {
		fmt.Printf("   Go pgx is %.2fx FASTER than QAIL!\n", qps/qailLocalQps)
	} else {
		fmt.Printf("   QAIL is %.2fx FASTER than Go pgx!\n", qailLocalQps/qps)
	}
}

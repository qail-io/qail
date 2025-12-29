// Go pgx Local SIMPLE QUERY Benchmark (no params)
// Matches QAIL raw SQL benchmark
//
// Run: go run pgx_simple_benchmark.go

package main

import (
	"context"
	"fmt"
	"os/user"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	QueriesPerBatch = 1000
	Batches         = 1000
)

func main() {
	currentUser, _ := user.Current()
	username := currentUser.Username
	
	connStr := fmt.Sprintf("postgres://%s@127.0.0.1:5432/swb_staging_local?sslmode=disable", username)
	
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		fmt.Printf("Connect error: %v\n", err)
		return
	}
	defer conn.Close(ctx)

	totalQueries := Batches * QueriesPerBatch

	fmt.Println("🚀 GO PGX SIMPLE QUERY BENCHMARK (no params)")
	fmt.Println("=============================================")
	fmt.Printf("Total queries: %d\n", totalQueries)
	fmt.Printf("Batch size:    %d\n", QueriesPerBatch)
	fmt.Printf("Batches:       %d\n", Batches)
	fmt.Println("\n⚠️  SIMPLE QUERY - NO PARAMS (like QAIL)!\n")

	// Pre-build simple query string (all queries in one string)
	var sb strings.Builder
	for i := 1; i <= QueriesPerBatch; i++ {
		limit := (i % 10) + 1
		sb.WriteString("SELECT id,name FROM harbors LIMIT ")
		sb.WriteString(strconv.Itoa(limit))
		sb.WriteString(";")
	}
	queryString := sb.String()
	
	fmt.Printf("Query string size: %d bytes\n", len(queryString))

	// Warmup
	conn.Exec(ctx, "SELECT 1")

	fmt.Println("\n📊 Running simple query benchmark...")

	start := time.Now()
	successfulQueries := 0

	for batch := 0; batch < Batches; batch++ {
		if batch%100 == 0 {
			fmt.Printf("   Batch %d/%d\n", batch, Batches)
		}

		// Execute ALL queries in one simple query call
		_, err := conn.Exec(ctx, queryString)
		if err != nil {
			fmt.Printf("Query error: %v\n", err)
			return
		}
		
		successfulQueries += QueriesPerBatch
	}

	elapsed := time.Since(start)

	qps := float64(totalQueries) / elapsed.Seconds()
	perQueryNs := elapsed.Nanoseconds() / int64(totalQueries)

	fmt.Println("\n📈 Results:")
	fmt.Println("┌──────────────────────────────────────────┐")
	fmt.Println("│ GO PGX SIMPLE - ONE MILLION QUERIES      │")
	fmt.Println("├──────────────────────────────────────────┤")
	fmt.Printf("│ Total Time:     %23s │\n", elapsed.Round(time.Millisecond))
	fmt.Printf("│ Queries/Second: %23.0f │\n", qps)
	fmt.Printf("│ Per Query:      %20dns │\n", perQueryNs)
	fmt.Printf("│ Successful:     %23d │\n", successfulQueries)
	fmt.Println("└──────────────────────────────────────────┘")

	fmt.Printf("\n📊 vs QAIL raw SQL (98,904 q/s):\n")
	if qps > 98904.0 {
		fmt.Printf("   Go is %.2fx faster than QAIL\n", qps/98904.0)
	} else {
		fmt.Printf("   🎉 QAIL is %.2fx faster than Go!\n", 98904.0/qps)
	}
}

// COMPLEX QUERY BENCHMARK - Go pgx
//
// Tests same queries as million_complex.rs for comparison:
// - Simple SELECT
// - SELECT with WHERE LIKE
// - SELECT with ORDER BY
// - SELECT with many columns
//
// Run: cd qail-pg/examples && go run pgx_complex_benchmark.go

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	TOTAL_QUERIES     = 100000
	QUERIES_PER_BATCH = 100
	BATCHES           = TOTAL_QUERIES / QUERIES_PER_BATCH
)

func main() {
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, "postgres://user@127.0.0.1:5432/example_staging")
	if err != nil {
		panic(err)
	}
	defer conn.Close(ctx)

	fmt.Println("🚀 COMPLEX QUERY BENCHMARK - Go pgx")
	fmt.Println("====================================")
	fmt.Printf("Total queries:    %12d\n", TOTAL_QUERIES)
	fmt.Printf("Batch size:       %12d\n", QUERIES_PER_BATCH)
	fmt.Printf("Batches:          %12d\n", BATCHES)
	fmt.Println("\n📊 Query Types:\n")

	// ========================
	// Test 1: Simple SELECT
	// ========================
	fmt.Println("1️⃣  SIMPLE SELECT (baseline)")

	start := time.Now()
	for batch := 0; batch < BATCHES; batch++ {
		b := &pgx.Batch{}
		for i := 1; i <= QUERIES_PER_BATCH; i++ {
			limit := (i % 10) + 1
			b.Queue("SELECT id, name FROM harbors LIMIT $1", limit)
		}
		br := conn.SendBatch(ctx, b)
		for i := 0; i < QUERIES_PER_BATCH; i++ {
			_, err := br.Exec()
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				return
			}
		}
		br.Close()
	}
	simpleElapsed := time.Since(start)
	simpleQPS := float64(TOTAL_QUERIES) / simpleElapsed.Seconds()
	fmt.Printf("   ✅ %.0f q/s (%.2fs)\n", simpleQPS, simpleElapsed.Seconds())

	// ========================
	// Test 2: SELECT with WHERE
	// ========================
	fmt.Println("\n2️⃣  SELECT with WHERE clause")

	start = time.Now()
	for batch := 0; batch < BATCHES; batch++ {
		b := &pgx.Batch{}
		for i := 1; i <= QUERIES_PER_BATCH; i++ {
			pattern := fmt.Sprintf("%%harbor%d%%", i%10)
			b.Queue("SELECT id, name, country, latitude, longitude FROM harbors WHERE name LIKE $1 LIMIT 10", pattern)
		}
		br := conn.SendBatch(ctx, b)
		for i := 0; i < QUERIES_PER_BATCH; i++ {
			_, err := br.Exec()
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				return
			}
		}
		br.Close()
	}
	whereElapsed := time.Since(start)
	whereQPS := float64(TOTAL_QUERIES) / whereElapsed.Seconds()
	fmt.Printf("   ✅ %.0f q/s (%.2fs)\n", whereQPS, whereElapsed.Seconds())

	// ========================
	// Test 3: SELECT with ORDER BY
	// ========================
	fmt.Println("\n3️⃣  SELECT with ORDER BY")

	start = time.Now()
	for batch := 0; batch < BATCHES; batch++ {
		b := &pgx.Batch{}
		for i := 1; i <= QUERIES_PER_BATCH; i++ {
			pattern := fmt.Sprintf("%%%d%%", i%10)
			b.Queue("SELECT id, name, country FROM harbors WHERE name LIKE $1 ORDER BY name ASC LIMIT 20", pattern)
		}
		br := conn.SendBatch(ctx, b)
		for i := 0; i < QUERIES_PER_BATCH; i++ {
			_, err := br.Exec()
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				return
			}
		}
		br.Close()
	}
	orderElapsed := time.Since(start)
	orderQPS := float64(TOTAL_QUERIES) / orderElapsed.Seconds()
	fmt.Printf("   ✅ %.0f q/s (%.2fs)\n", orderQPS, orderElapsed.Seconds())

	// ========================
	// Test 4: Many columns
	// ========================
	fmt.Println("\n4️⃣  SELECT with MANY columns")

	start = time.Now()
	for batch := 0; batch < BATCHES; batch++ {
		b := &pgx.Batch{}
		for i := 1; i <= QUERIES_PER_BATCH; i++ {
			pattern := fmt.Sprintf("%%test%d%%", i%5)
			b.Queue("SELECT id, name, country, latitude, longitude, timezone, created_at, updated_at FROM harbors WHERE name LIKE $1", pattern)
		}
		br := conn.SendBatch(ctx, b)
		for i := 0; i < QUERIES_PER_BATCH; i++ {
			_, err := br.Exec()
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				return
			}
		}
		br.Close()
	}
	manyElapsed := time.Since(start)
	manyQPS := float64(TOTAL_QUERIES) / manyElapsed.Seconds()
	fmt.Printf("   ✅ %.0f q/s (%.2fs)\n", manyQPS, manyElapsed.Seconds())

	// ========================
	// Summary
	// ========================
	fmt.Println("\n📈 SUMMARY (Go pgx):")
	fmt.Println("┌──────────────────────────────────────────┐")
	fmt.Println("│ Query Type          │ Q/s      │ vs Base │")
	fmt.Println("├──────────────────────────────────────────┤")
	fmt.Printf("│ Simple SELECT       │ %8.0f │  1.00x  │\n", simpleQPS)
	fmt.Printf("│ + WHERE clause      │ %8.0f │  %.2fx  │\n", whereQPS, whereQPS/simpleQPS)
	fmt.Printf("│ + ORDER BY          │ %8.0f │  %.2fx  │\n", orderQPS, orderQPS/simpleQPS)
	fmt.Printf("│ + Many columns      │ %8.0f │  %.2fx  │\n", manyQPS, manyQPS/simpleQPS)
	fmt.Println("└──────────────────────────────────────────┘")
}

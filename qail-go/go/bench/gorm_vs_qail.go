// QAIL-GO vs GORM Benchmark
//
// Tests if QAIL-GO is really 3-5x faster than GORM ORM
//
// Run:
//   cd qail-go/go/bench && go mod init bench && go get gorm.io/gorm gorm.io/driver/postgres
//   go run gorm_vs_qail.go

package main

import (
	"fmt"
	"os"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	qail "github.com/qail-lang/qail-go"
)

type Harbor struct {
	ID   int    `gorm:"primaryKey"`
	Name string
}

const (
	QUERIES_PER_BATCH = 100 // GORM is slower, use smaller batches
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

	fmt.Println("🏁 QAIL-GO vs GORM BENCHMARK")
	fmt.Println("============================")
	fmt.Printf("Total queries: %d\n", TOTAL_QUERIES)
	fmt.Printf("Batch size:    %d\n", QUERIES_PER_BATCH)
	fmt.Printf("Batches:       %d\n\n", BATCHES)

	// ========== GORM BENCHMARK ==========
	fmt.Println("📊 [1/2] GORM (ORM)...")
	gormQPS := benchmarkGORM(host, port, user, database)

	// ========== QAIL BENCHMARK ==========
	fmt.Println("\n📊 [2/2] QAIL-GO (Prepared)...")
	qailQPS := benchmarkQAIL(host, port, user, database)

	// ========== RESULTS ==========
	fmt.Println("\n📈 RESULTS:")
	fmt.Println("┌──────────────────────────────────────────┐")
	fmt.Println("│ QAIL-GO vs GORM BENCHMARK                │")
	fmt.Println("├──────────────────────────────────────────┤")
	fmt.Printf("│ GORM:             %23.0f q/s │\n", gormQPS)
	fmt.Printf("│ QAIL-GO:          %23.0f q/s │\n", qailQPS)
	fmt.Println("├──────────────────────────────────────────┤")

	if qailQPS > gormQPS {
		speedup := qailQPS / gormQPS
		fmt.Printf("│ 🏆 QAIL-GO is %.1fx FASTER than GORM!   │\n", speedup)
	} else {
		ratio := gormQPS / qailQPS
		fmt.Printf("│ ❌ GORM is %.1fx faster than QAIL-GO    │\n", ratio)
	}
	fmt.Println("└──────────────────────────────────────────┘")
}

func benchmarkGORM(host, port, user, database string) float64 {
	dsn := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable", host, port, user, database)
	
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		fmt.Printf("GORM connect error: %v\n", err)
		return 0
	}
	sqlDB, _ := db.DB()
	defer sqlDB.Close()

	// Warmup
	var harbor Harbor
	db.First(&harbor)

	start := time.Now()
	successfulQueries := 0

	for batch := 0; batch < BATCHES; batch++ {
		for i := 0; i < QUERIES_PER_BATCH; i++ {
			limit := (i % 10) + 1
			var harbors []Harbor
			if err := db.Limit(limit).Find(&harbors).Error; err != nil {
				fmt.Printf("GORM query error: %v\n", err)
				return 0
			}
			successfulQueries++
		}

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

	// Prepare batch of 100 queries ONCE
	prepared := driver.PrepareBatchN("harbors", "id,name", QUERIES_PER_BATCH)
	if prepared == nil {
		fmt.Println("Failed to prepare batch")
		return 0
	}
	fmt.Println("   ✅ Prepared batch")

	start := time.Now()
	successfulQueries := 0

	for batch := 0; batch < BATCHES; batch++ {
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

// For sequential comparison (fair test)
func benchmarkGORMSequential(host, port, user, database string) float64 {
	dsn := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable", host, port, user, database)
	
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		fmt.Printf("GORM connect error: %v\n", err)
		return 0
	}
	sqlDB, _ := db.DB()
	defer sqlDB.Close()

	const SEQ_QUERIES = 1000

	start := time.Now()
	for i := 0; i < SEQ_QUERIES; i++ {
		limit := (i % 10) + 1
		var harbors []Harbor
		db.Limit(limit).Find(&harbors)
	}
	elapsed := time.Since(start)
	
	return float64(SEQ_QUERIES) / elapsed.Seconds()
}

func benchmarkQAILSequential(driver *qail.Driver) float64 {
	const SEQ_QUERIES = 1000

	start := time.Now()
	for i := 0; i < SEQ_QUERIES; i++ {
		limit := int64((i % 10) + 1)
		cmd := qail.Get("harbors").
			Columns("id", "name").
			Limit(limit)
		driver.Execute(cmd)
		cmd.Free()
	}
	elapsed := time.Since(start)
	
	return float64(SEQ_QUERIES) / elapsed.Seconds()
}

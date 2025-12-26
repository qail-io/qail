// Example: Basic QAIL-Go usage
//
// Build:
//   cd qail-go/go && go build -o example ./examples/basic.go
//
// Run:
//   ./example

package main

import (
	"fmt"
	"os"

	qail "github.com/qail-lang/qail-go"
)

func main() {
	// Connect to PostgreSQL
	driver, err := qail.NewDriver(qail.Config{
		Host:     "localhost",
		Port:     "5432",
		User:     "postgres",
		Database: "postgres",
		Password: os.Getenv("PGPASSWORD"),
		PoolSize: 5,
	})
	if err != nil {
		fmt.Printf("Failed to create driver: %v\n", err)
		os.Exit(1)
	}
	defer driver.Close()

	// Build AST-native query (no SQL strings!)
	cmd := qail.Get("users").
		Columns("id", "name", "email").
		Filter("active", qail.Eq, true).
		Limit(10)
	defer cmd.Free()

	fmt.Println("📦 Wire bytes (first 50):", cmd.Encode()[:50])

	// Execute query
	rows, err := driver.FetchAll(cmd)
	if err != nil {
		fmt.Printf("Query failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("✅ Got %d rows\n", len(rows))
	for i, row := range rows {
		fmt.Printf("  [%d] id=%d, name=%s\n", i, row.GetInt(0), row.GetString(1))
	}
}

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	connString = "host=127.0.0.1 port=5432 user=orion dbname=example_staging sslmode=disable"
	batchSize  = 10_000
	iterations = 5
	poolSize   = 10
	sqlByID    = "SELECT id, name FROM harbors WHERE id = $1"
)

type preparedCall struct {
	stmt   string
	params [][]byte
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: pgx_benchmark [--mode strict|once|single|pipeline|pool10] [--workload literal|param] [--plain]\n")
	flag.PrintDefaults()
}

func median(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	mid := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[mid]
	}
	return (sorted[mid-1] + sorted[mid]) / 2
}

func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	rank := int(float64(len(sorted))*p + 0.999999999)
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}

func prepareTemplates(p *pgconn.Pipeline, templates map[string]string, orderedNames []string) error {
	for _, name := range orderedNames {
		p.SendPrepare(name, templates[name], nil)
	}
	if err := p.Sync(); err != nil {
		return err
	}
	expected := len(orderedNames)
	seen := 0
	for {
		results, err := p.GetResults()
		if err != nil {
			return err
		}
		switch r := results.(type) {
		case *pgconn.StatementDescription:
			_ = r
			seen++
		case *pgconn.PipelineSync:
			if seen != expected {
				return fmt.Errorf("prepare count mismatch: got %d want %d", seen, expected)
			}
			return nil
		case nil:
			continue
		default:
			return fmt.Errorf("unexpected prepare result type %T", r)
		}
	}
}

func runOnce(p *pgconn.Pipeline, calls []preparedCall) error {
	for _, call := range calls {
		p.SendQueryPrepared(call.stmt, call.params, nil, nil)
	}
	if err := p.Sync(); err != nil {
		return err
	}
	expected := len(calls)
	completed := 0
	for {
		results, err := p.GetResults()
		if err != nil {
			return err
		}
		switch r := results.(type) {
		case *pgconn.ResultReader:
			if _, err := r.Close(); err != nil {
				return err
			}
			completed++
		case *pgconn.PipelineSync:
			if completed != expected {
				return fmt.Errorf("completed mismatch: got %d want %d", completed, expected)
			}
			return nil
		case nil:
			continue
		default:
			return fmt.Errorf("unexpected result type %T", r)
		}
	}
}

func runPreparedPipelineBenchmark(calls []preparedCall, templates map[string]string, orderedNames []string) (float64, error) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return 0, err
	}
	defer conn.Close(ctx)

	p := conn.PgConn().StartPipeline(ctx)
	defer p.Close()

	if err := prepareTemplates(p, templates, orderedNames); err != nil {
		return 0, err
	}

	// Warmup (untimed)
	if err := runOnce(p, calls); err != nil {
		return 0, err
	}

	total := time.Duration(0)
	for i := 0; i < iterations; i++ {
		start := time.Now()
		if err := runOnce(p, calls); err != nil {
			return 0, err
		}
		total += time.Since(start)
	}

	qps := float64(len(calls)*iterations) / total.Seconds()
	return qps, nil
}

func buildParamValues(total int) [][]byte {
	params := make([][]byte, 0, total)
	for i := 1; i <= total; i++ {
		id := (i % 10_000) + 1
		params = append(params, []byte(fmt.Sprintf("%d", id)))
	}
	return params
}

func runSinglePreparedOnce(conn *pgconn.PgConn, stmtName string, params [][]byte) error {
	ctx := context.Background()
	for _, p := range params {
		rr := conn.ExecPrepared(ctx, stmtName, [][]byte{p}, nil, nil)
		if _, err := rr.Close(); err != nil {
			return err
		}
	}
	return nil
}

func runSingleMode() (float64, error) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return 0, err
	}
	defer conn.Close(ctx)

	pgConn := conn.PgConn()
	if _, err := pgConn.Prepare(ctx, "single_stmt", sqlByID, nil); err != nil {
		return 0, err
	}

	params := buildParamValues(batchSize)
	if err := runSinglePreparedOnce(pgConn, "single_stmt", params); err != nil {
		return 0, err
	}

	total := time.Duration(0)
	for i := 0; i < iterations; i++ {
		start := time.Now()
		if err := runSinglePreparedOnce(pgConn, "single_stmt", params); err != nil {
			return 0, err
		}
		total += time.Since(start)
	}

	qps := float64(batchSize*iterations) / total.Seconds()
	return qps, nil
}

func runPipelineMode() (float64, error) {
	calls, templates, ordered := buildParameterizedWorkload()
	return runPreparedPipelineBenchmark(calls, templates, ordered)
}

func runPool10Mode() (float64, error) {
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return 0, err
	}
	cfg.MaxConns = poolSize
	cfg.MinConns = poolSize

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return 0, err
	}
	defer pool.Close()

	perWorker := batchSize / poolSize
	workerParams := make([][][]byte, poolSize)
	for w := 0; w < poolSize; w++ {
		start := w * perWorker
		vals := make([][]byte, 0, perWorker)
		for i := 0; i < perWorker; i++ {
			id := ((start + i) % 10_000) + 1
			vals = append(vals, []byte(fmt.Sprintf("%d", id)))
		}
		workerParams[w] = vals
	}

	startBarrier := make(chan struct{})

	var wg sync.WaitGroup
	var once sync.Once
	var workerErr error

	for w := 0; w < poolSize; w++ {
		params := workerParams[w]
		wg.Add(1)
		go func(idx int, vals [][]byte) {
			defer wg.Done()

			poolConn, err := pool.Acquire(ctx)
			if err != nil {
				once.Do(func() { workerErr = err })
				return
			}
			defer poolConn.Release()

			pgConn := poolConn.Conn().PgConn()
			stmtName := fmt.Sprintf("pool_stmt_%d", idx)
			if _, err := pgConn.Prepare(ctx, stmtName, sqlByID, nil); err != nil {
				once.Do(func() { workerErr = err })
				return
			}

			if err := runSinglePreparedOnce(pgConn, stmtName, vals); err != nil {
				once.Do(func() { workerErr = err })
				return
			}

			<-startBarrier
			for i := 0; i < iterations; i++ {
				if err := runSinglePreparedOnce(pgConn, stmtName, vals); err != nil {
					once.Do(func() { workerErr = err })
					return
				}
			}
		}(w, params)
	}

	close(startBarrier)
	start := time.Now()
	wg.Wait()
	elapsed := time.Since(start)

	if workerErr != nil {
		return 0, workerErr
	}

	qps := float64(batchSize*iterations) / elapsed.Seconds()
	return qps, nil
}

func buildLiteralWorkload() ([]preparedCall, map[string]string, []string) {
	templates := map[string]string{}
	ordered := make([]string, 0, 10)
	calls := make([]preparedCall, 0, batchSize)

	for i := 1; i <= batchSize; i++ {
		limit := (i % 10) + 1
		name := fmt.Sprintf("lit_%d", limit)
		if _, ok := templates[name]; !ok {
			templates[name] = fmt.Sprintf("SELECT id, name FROM harbors LIMIT %d", limit)
			ordered = append(ordered, name)
		}
		calls = append(calls, preparedCall{stmt: name, params: nil})
	}

	return calls, templates, ordered
}

func buildParameterizedWorkload() ([]preparedCall, map[string]string, []string) {
	templates := map[string]string{
		"param_id": "SELECT id, name FROM harbors WHERE id = $1",
	}
	ordered := []string{"param_id"}
	calls := make([]preparedCall, 0, batchSize)

	for i := 1; i <= batchSize; i++ {
		id := (i % 10_000) + 1
		calls = append(calls, preparedCall{
			stmt:   "param_id",
			params: [][]byte{[]byte(fmt.Sprintf("%d", id))},
		})
	}

	return calls, templates, ordered
}

func workloadFromName(name string) ([]preparedCall, map[string]string, []string, string, error) {
	switch name {
	case "literal":
		calls, templates, ordered := buildLiteralWorkload()
		return calls, templates, ordered, "Workload A: template-cached literal LIMIT (0 bind params)", nil
	case "param", "parameterized":
		calls, templates, ordered := buildParameterizedWorkload()
		return calls, templates, ordered, "Workload B: template-cached parameterized filter (1 bind param)", nil
	default:
		return nil, nil, nil, "", fmt.Errorf("unknown workload %q (expected literal or param)", name)
	}
}

func runStrict(name string, calls []preparedCall, templates map[string]string, orderedNames []string) (float64, float64, error) {
	orders := []bool{true, false, false, true}
	runs := make([]float64, 0, len(orders))

	fmt.Printf("  %s\n", name)
	for round, first := range orders {
		// Keep ABBA shape (A/B/B/A). For single-driver benchmark this just
		// forces repeated independent runs and preserves comparability with
		// existing strict harness style.
		_ = first
		qps, err := runPreparedPipelineBenchmark(calls, templates, orderedNames)
		if err != nil {
			return 0, 0, fmt.Errorf("round %d failed: %w", round+1, err)
		}
		runs = append(runs, qps)
		fmt.Printf("    Round %d: %8.0f q/s\n", round+1, qps)
	}

	return median(runs), percentile(runs, 0.95), nil
}

func main() {
	mode := flag.String("mode", "strict", "benchmark mode: strict, once, single, pipeline, or pool10")
	workload := flag.String("workload", "literal", "workload for --mode once: literal or param")
	plain := flag.Bool("plain", false, "print only numeric q/s in --mode once")
	flag.Usage = usage
	flag.Parse()

	switch *mode {
	case "single":
		qps, err := runSingleMode()
		if err != nil {
			panic(err)
		}
		if *plain {
			fmt.Printf("%.3f\n", qps)
		} else {
			fmt.Printf("single: %.0f q/s\n", qps)
		}
		return
	case "pipeline":
		qps, err := runPipelineMode()
		if err != nil {
			panic(err)
		}
		if *plain {
			fmt.Printf("%.3f\n", qps)
		} else {
			fmt.Printf("pipeline: %.0f q/s\n", qps)
		}
		return
	case "pool10":
		qps, err := runPool10Mode()
		if err != nil {
			panic(err)
		}
		if *plain {
			fmt.Printf("%.3f\n", qps)
		} else {
			fmt.Printf("pool10: %.0f q/s\n", qps)
		}
		return
	}

	if *mode == "once" {
		calls, templates, ordered, title, err := workloadFromName(*workload)
		if err != nil {
			panic(err)
		}
		qps, err := runPreparedPipelineBenchmark(calls, templates, ordered)
		if err != nil {
			panic(err)
		}
		if *plain {
			fmt.Printf("%.3f\n", qps)
		} else {
			fmt.Printf("%s: %.0f q/s\n", title, qps)
		}
		return
	}
	if *mode != "strict" {
		panic(fmt.Errorf("unknown mode %q (expected strict, once, single, pipeline, or pool10)", *mode))
	}

	fmt.Println("🏁 PGX STRICT BENCHMARK (pipeline + prepared)")
	fmt.Println("============================================")
	fmt.Printf("batch=%d iterations=%d (per round)\n\n", batchSize, iterations)

	litCalls, litTemplates, litOrdered := buildLiteralWorkload()
	paramCalls, paramTemplates, paramOrdered := buildParameterizedWorkload()

	litMedian, litP95, err := runStrict(
		"Workload A: template-cached literal LIMIT (0 bind params)",
		litCalls,
		litTemplates,
		litOrdered,
	)
	if err != nil {
		panic(err)
	}

	paramMedian, paramP95, err := runStrict(
		"Workload B: template-cached parameterized filter (1 bind param)",
		paramCalls,
		paramTemplates,
		paramOrdered,
	)
	if err != nil {
		panic(err)
	}

	fmt.Println("\n=== PGX SUMMARY ===")
	fmt.Printf("  literal median/p95:       %8.0f / %8.0f q/s\n", litMedian, litP95)
	fmt.Printf("  parameterized median/p95: %8.0f / %8.0f q/s\n", paramMedian, paramP95)
}

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	connString = "host=127.0.0.1 port=5432 user=orion dbname=example_staging sslmode=disable"
	poolSize   = 10

	sqlByID = "SELECT id, name FROM harbors WHERE id = $1"

	wideRowsSQL = "SELECT gs AS id, " +
		"('harbor-' || gs)::text AS name, " +
		"repeat(md5(gs::text), 4) AS bio, " +
		"repeat(md5((gs * 17)::text), 3) AS region, " +
		"(gs * 11) AS visits, " +
		"(gs % 2 = 0) AS active, " +
		"round((gs::numeric / 7.0), 3) AS ratio, " +
		"CASE WHEN gs % 5 = 0 THEN NULL ELSE repeat(md5((gs * 3)::text), 2) END AS optional_note " +
		"FROM generate_series(1, $1::int) AS gs"

	manyParamsParamCount = 32
	manyParamsSQL        = "SELECT " +
		"$1::int + $2::int + $3::int + $4::int + $5::int + $6::int + $7::int + $8::int + " +
		"$9::int + $10::int + $11::int + $12::int + $13::int + $14::int + $15::int + $16::int + " +
		"$17::int + $18::int + $19::int + $20::int + $21::int + $22::int + $23::int + $24::int + " +
		"$25::int + $26::int + $27::int + $28::int + $29::int + $30::int + $31::int + $32::int " +
		"AS total"

	pointBatchSize       = 10_000
	pointIterations      = 5
	wideRowsBatchSize    = 100
	wideRowsIterations   = 3
	manyParamsBatchSize  = 5_000
	manyParamsIterations = 5

	fnvOffset = uint64(0xcbf29ce484222325)
	fnvPrime  = uint64(1099511628211)
)

type preparedCall struct {
	stmt   string
	params [][]byte
}

type resultMode int

const (
	resultModeCompleteOnly resultMode = iota
	resultModeScalarInt
	resultModeWideRows
)

type modeWorkload struct {
	name       string
	sql        string
	batchSize  int
	iterations int
	mode       resultMode
}

type batchStats struct {
	completed int
	rows      int
	bytes     int
	checksum  uint64
}

func (s *batchStats) add(other batchStats) {
	s.completed += other.completed
	s.rows += other.rows
	s.bytes += other.bytes
	s.checksum += other.checksum
}

type benchmarkResult struct {
	qps        float64
	rowsPerSec float64
	mibPerSec  float64
	hasRows    bool
	hasMiB     bool
	checksum   uint64
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: pgx_benchmark [--mode strict|once|single|pipeline|pool10] [--workload literal|param|point|wide_rows|many_params] [--plain]\n")
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

func modeWorkloadFromName(name string) (modeWorkload, error) {
	switch name {
	case "point", "lookup":
		return modeWorkload{
			name:       "point",
			sql:        sqlByID,
			batchSize:  pointBatchSize,
			iterations: pointIterations,
			mode:       resultModeCompleteOnly,
		}, nil
	case "wide_rows", "wide":
		return modeWorkload{
			name:       "wide_rows",
			sql:        wideRowsSQL,
			batchSize:  wideRowsBatchSize,
			iterations: wideRowsIterations,
			mode:       resultModeWideRows,
		}, nil
	case "many_params", "params":
		return modeWorkload{
			name:       "many_params",
			sql:        manyParamsSQL,
			batchSize:  manyParamsBatchSize,
			iterations: manyParamsIterations,
			mode:       resultModeScalarInt,
		}, nil
	default:
		return modeWorkload{}, fmt.Errorf("unknown workload %q (expected point, wide_rows, or many_params)", name)
	}
}

func buildModeParamBatch(spec modeWorkload) [][][]byte {
	switch spec.name {
	case "point":
		params := make([][][]byte, 0, spec.batchSize)
		for i := 1; i <= spec.batchSize; i++ {
			id := (i % 10_000) + 1
			params = append(params, [][]byte{[]byte(strconv.Itoa(id))})
		}
		return params
	case "wide_rows":
		rowCounts := []string{"128", "256", "384", "512"}
		params := make([][][]byte, 0, spec.batchSize)
		for i := 0; i < spec.batchSize; i++ {
			params = append(params, [][]byte{[]byte(rowCounts[i%len(rowCounts)])})
		}
		return params
	case "many_params":
		cache := make([][]byte, 256)
		for i := range cache {
			cache[i] = []byte(strconv.Itoa(i + 1))
		}

		params := make([][][]byte, 0, spec.batchSize)
		for queryIdx := 0; queryIdx < spec.batchSize; queryIdx++ {
			row := make([][]byte, manyParamsParamCount)
			for paramIdx := 0; paramIdx < manyParamsParamCount; paramIdx++ {
				valueIdx := (queryIdx + paramIdx*7) % len(cache)
				row[paramIdx] = cache[valueIdx]
			}
			params = append(params, row)
		}
		return params
	default:
		return nil
	}
}

func buildModeCalls(spec modeWorkload) ([]preparedCall, map[string]string, []string, [][][]byte) {
	params := buildModeParamBatch(spec)
	calls := make([]preparedCall, 0, len(params))
	for _, paramSet := range params {
		calls = append(calls, preparedCall{
			stmt:   "mode_stmt",
			params: paramSet,
		})
	}

	return calls, map[string]string{"mode_stmt": spec.sql}, []string{"mode_stmt"}, params
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

func consumeValues(mode resultMode, values [][]byte, stats *batchStats) {
	switch mode {
	case resultModeCompleteOnly:
		return
	case resultModeScalarInt:
		stats.rows++
		if len(values) == 0 || values[0] == nil {
			stats.checksum++
			return
		}

		value := values[0]
		stats.bytes += len(value)
		parsed, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			parsed = int64(len(value))
		}
		stats.checksum += uint64(parsed)
	case resultModeWideRows:
		rowHash := fnvOffset
		for idx, value := range values {
			if value == nil {
				rowHash = mixHash(rowHash, []byte("NULL"))
				rowHash += uint64(idx)
				continue
			}

			stats.bytes += len(value)
			switch idx {
			case 0, 4:
				parsed, err := strconv.ParseInt(string(value), 10, 64)
				if err != nil {
					parsed = int64(len(value))
				}
				rowHash += uint64(parsed)
			case 5:
				if len(value) > 0 && (value[0] == 't' || value[0] == 'T') {
					rowHash++
				}
			case 6:
				parsed, err := strconv.ParseFloat(string(value), 64)
				if err == nil {
					rowHash += uint64(parsed * 1000.0)
				}
			default:
				rowHash = mixHash(rowHash, value)
			}
		}
		stats.rows++
		stats.checksum += rowHash
	}
}

func consumeResultReader(rr *pgconn.ResultReader, mode resultMode) (batchStats, error) {
	stats := batchStats{}

	if mode == resultModeCompleteOnly {
		_, err := rr.Close()
		if err != nil {
			return batchStats{}, err
		}
		stats.completed = 1
		return stats, nil
	}

	for rr.NextRow() {
		consumeValues(mode, rr.Values(), &stats)
	}

	_, err := rr.Close()
	if err != nil {
		return batchStats{}, err
	}
	stats.completed = 1
	return stats, nil
}

func runPipelineOnce(p *pgconn.Pipeline, calls []preparedCall, mode resultMode) (batchStats, error) {
	for _, call := range calls {
		p.SendQueryPrepared(call.stmt, call.params, nil, nil)
	}
	if err := p.Sync(); err != nil {
		return batchStats{}, err
	}

	expected := len(calls)
	stats := batchStats{}

	for {
		results, err := p.GetResults()
		if err != nil {
			return batchStats{}, err
		}

		switch r := results.(type) {
		case *pgconn.ResultReader:
			readerStats, err := consumeResultReader(r, mode)
			if err != nil {
				return batchStats{}, err
			}
			stats.add(readerStats)
		case *pgconn.PipelineSync:
			if stats.completed != expected {
				return batchStats{}, fmt.Errorf("completed mismatch: got %d want %d", stats.completed, expected)
			}
			return stats, nil
		case nil:
			continue
		default:
			return batchStats{}, fmt.Errorf("unexpected result type %T", r)
		}
	}
}

func makeBenchmarkResult(stats batchStats, elapsed time.Duration) benchmarkResult {
	seconds := elapsed.Seconds()
	result := benchmarkResult{
		qps:      float64(stats.completed) / seconds,
		checksum: stats.checksum,
	}
	if stats.rows > 0 {
		result.hasRows = true
		result.rowsPerSec = float64(stats.rows) / seconds
	}
	if stats.bytes > 0 {
		result.hasMiB = true
		result.mibPerSec = (float64(stats.bytes) / (1024.0 * 1024.0)) / seconds
	}
	return result
}

func runPreparedPipelineModeBenchmark(
	calls []preparedCall,
	templates map[string]string,
	orderedNames []string,
	mode resultMode,
	iterations int,
) (benchmarkResult, error) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return benchmarkResult{}, err
	}
	defer conn.Close(ctx)

	p := conn.PgConn().StartPipeline(ctx)
	defer p.Close()

	if err := prepareTemplates(p, templates, orderedNames); err != nil {
		return benchmarkResult{}, err
	}

	warmup, err := runPipelineOnce(p, calls, mode)
	if err != nil {
		return benchmarkResult{}, err
	}
	if warmup.completed != len(calls) {
		return benchmarkResult{}, fmt.Errorf("warmup completed %d queries, expected %d", warmup.completed, len(calls))
	}

	total := time.Duration(0)
	aggregate := batchStats{}
	for i := 0; i < iterations; i++ {
		start := time.Now()
		stats, err := runPipelineOnce(p, calls, mode)
		if err != nil {
			return benchmarkResult{}, err
		}
		total += time.Since(start)
		if stats.completed != len(calls) {
			return benchmarkResult{}, fmt.Errorf("run completed %d queries, expected %d", stats.completed, len(calls))
		}
		aggregate.add(stats)
	}

	return makeBenchmarkResult(aggregate, total), nil
}

func runPreparedPipelineBenchmark(calls []preparedCall, templates map[string]string, orderedNames []string) (float64, error) {
	result, err := runPreparedPipelineModeBenchmark(calls, templates, orderedNames, resultModeCompleteOnly, 5)
	if err != nil {
		return 0, err
	}
	return result.qps, nil
}

func runSinglePreparedOnce(conn *pgconn.PgConn, stmtName string, params [][][]byte, mode resultMode) (batchStats, error) {
	ctx := context.Background()
	stats := batchStats{}

	for _, paramSet := range params {
		rr := conn.ExecPrepared(ctx, stmtName, paramSet, nil, nil)
		readerStats, err := consumeResultReader(rr, mode)
		if err != nil {
			return batchStats{}, err
		}
		stats.add(readerStats)
	}

	return stats, nil
}

func runSingleMode(spec modeWorkload) (benchmarkResult, error) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return benchmarkResult{}, err
	}
	defer conn.Close(ctx)

	pgConn := conn.PgConn()
	if _, err := pgConn.Prepare(ctx, "single_stmt", spec.sql, nil); err != nil {
		return benchmarkResult{}, err
	}

	params := buildModeParamBatch(spec)
	warmup, err := runSinglePreparedOnce(pgConn, "single_stmt", params, spec.mode)
	if err != nil {
		return benchmarkResult{}, err
	}
	if warmup.completed != len(params) {
		return benchmarkResult{}, fmt.Errorf("warmup completed %d queries, expected %d", warmup.completed, len(params))
	}

	total := time.Duration(0)
	aggregate := batchStats{}
	for i := 0; i < spec.iterations; i++ {
		start := time.Now()
		stats, err := runSinglePreparedOnce(pgConn, "single_stmt", params, spec.mode)
		if err != nil {
			return benchmarkResult{}, err
		}
		total += time.Since(start)
		if stats.completed != len(params) {
			return benchmarkResult{}, fmt.Errorf("run completed %d queries, expected %d", stats.completed, len(params))
		}
		aggregate.add(stats)
	}

	return makeBenchmarkResult(aggregate, total), nil
}

func runPipelineMode(spec modeWorkload) (benchmarkResult, error) {
	calls, templates, ordered, _ := buildModeCalls(spec)
	return runPreparedPipelineModeBenchmark(calls, templates, ordered, spec.mode, spec.iterations)
}

func runPool10Mode(spec modeWorkload) (benchmarkResult, error) {
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return benchmarkResult{}, err
	}
	cfg.MaxConns = poolSize
	cfg.MinConns = poolSize

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return benchmarkResult{}, err
	}
	defer pool.Close()

	params := buildModeParamBatch(spec)
	if len(params)%poolSize != 0 {
		return benchmarkResult{}, fmt.Errorf("workload %q produced %d params, not divisible by pool size %d", spec.name, len(params), poolSize)
	}

	perWorker := len(params) / poolSize
	workerParams := make([][][][]byte, poolSize)
	for w := 0; w < poolSize; w++ {
		startIdx := w * perWorker
		workerParams[w] = params[startIdx : startIdx+perWorker]
	}

	startSignal := make(chan struct{})
	readyCh := make(chan struct{}, poolSize)
	statsCh := make(chan batchStats, poolSize)
	errCh := make(chan error, poolSize)

	var wg sync.WaitGroup
	for w := 0; w < poolSize; w++ {
		params := workerParams[w]
		wg.Add(1)
		go func(idx int, vals [][][]byte) {
			defer wg.Done()

			poolConn, err := pool.Acquire(ctx)
			if err != nil {
				readyCh <- struct{}{}
				errCh <- err
				return
			}
			defer poolConn.Release()

			pgConn := poolConn.Conn().PgConn()
			stmtName := fmt.Sprintf("pool_stmt_%d", idx)
			if _, err := pgConn.Prepare(ctx, stmtName, spec.sql, nil); err != nil {
				readyCh <- struct{}{}
				errCh <- err
				return
			}

			warmup, err := runSinglePreparedOnce(pgConn, stmtName, vals, spec.mode)
			if err != nil {
				readyCh <- struct{}{}
				errCh <- err
				return
			}
			if warmup.completed != len(vals) {
				readyCh <- struct{}{}
				errCh <- fmt.Errorf("worker %d warmup completed %d queries, expected %d", idx, warmup.completed, len(vals))
				return
			}

			readyCh <- struct{}{}
			<-startSignal

			measured := batchStats{}
			for i := 0; i < spec.iterations; i++ {
				stats, err := runSinglePreparedOnce(pgConn, stmtName, vals, spec.mode)
				if err != nil {
					errCh <- err
					return
				}
				if stats.completed != len(vals) {
					errCh <- fmt.Errorf("worker %d run completed %d queries, expected %d", idx, stats.completed, len(vals))
					return
				}
				measured.add(stats)
			}

			statsCh <- measured
		}(w, params)
	}

	for i := 0; i < poolSize; i++ {
		<-readyCh
	}

	start := time.Now()
	close(startSignal)
	wg.Wait()
	elapsed := time.Since(start)

	select {
	case err := <-errCh:
		return benchmarkResult{}, err
	default:
	}

	close(statsCh)
	aggregate := batchStats{}
	for stats := range statsCh {
		aggregate.add(stats)
	}

	return makeBenchmarkResult(aggregate, elapsed), nil
}

func buildLiteralWorkload() ([]preparedCall, map[string]string, []string) {
	templates := map[string]string{}
	ordered := make([]string, 0, 10)
	calls := make([]preparedCall, 0, pointBatchSize)

	for i := 1; i <= pointBatchSize; i++ {
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
	calls := make([]preparedCall, 0, pointBatchSize)

	for i := 1; i <= pointBatchSize; i++ {
		id := (i % 10_000) + 1
		calls = append(calls, preparedCall{
			stmt:   "param_id",
			params: [][]byte{[]byte(strconv.Itoa(id))},
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
	for round := range orders {
		qps, err := runPreparedPipelineBenchmark(calls, templates, orderedNames)
		if err != nil {
			return 0, 0, fmt.Errorf("round %d failed: %w", round+1, err)
		}
		runs = append(runs, qps)
		fmt.Printf("    Round %d: %8.0f q/s\n", round+1, qps)
	}

	return median(runs), percentile(runs, 0.95), nil
}

func printModeResult(label string, result benchmarkResult, plain bool, mode resultMode) {
	if plain {
		fmt.Printf("%.3f\n", result.qps)
		return
	}

	fmt.Printf("%s: %.0f q/s", label, result.qps)
	if result.hasRows {
		fmt.Printf(" | %.0f rows/s", result.rowsPerSec)
	}
	if result.hasMiB {
		fmt.Printf(" | %.2f MiB/s", result.mibPerSec)
	}
	if mode != resultModeCompleteOnly {
		fmt.Printf(" | checksum=0x%x", result.checksum)
	}
	fmt.Println()
}

func mixHash(seed uint64, bytes []byte) uint64 {
	hash := seed
	for _, b := range bytes {
		hash ^= uint64(b)
		hash *= fnvPrime
	}
	return hash
}

func main() {
	mode := flag.String("mode", "strict", "benchmark mode: strict, once, single, pipeline, or pool10")
	workload := flag.String("workload", "", "workload name: strict/once use literal|param; single/pipeline/pool10 use point|wide_rows|many_params")
	plain := flag.Bool("plain", false, "print only numeric q/s in single-run modes")
	flag.Usage = usage
	flag.Parse()

	switch *mode {
	case "single", "pipeline", "pool10":
		workloadName := *workload
		if workloadName == "" {
			workloadName = "point"
		}

		spec, err := modeWorkloadFromName(workloadName)
		if err != nil {
			panic(err)
		}

		var result benchmarkResult
		switch *mode {
		case "single":
			result, err = runSingleMode(spec)
		case "pipeline":
			result, err = runPipelineMode(spec)
		case "pool10":
			result, err = runPool10Mode(spec)
		}
		if err != nil {
			panic(err)
		}

		printModeResult(fmt.Sprintf("%s/%s", *mode, spec.name), result, *plain, spec.mode)
		return
	case "once":
		workloadName := *workload
		if workloadName == "" {
			workloadName = "literal"
		}
		calls, templates, ordered, title, err := workloadFromName(workloadName)
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
	case "strict":
	default:
		panic(fmt.Errorf("unknown mode %q (expected strict, once, single, pipeline, or pool10)", *mode))
	}

	fmt.Println("🏁 PGX STRICT BENCHMARK (pipeline + prepared)")
	fmt.Println("============================================")
	fmt.Printf("batch=%d iterations=%d (per round)\n\n", pointBatchSize, 5)

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

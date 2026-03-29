package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	poolSize = 10

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

	largeRowsSQL = "SELECT id, name, bio, region, visits, active, ratio, optional_note " +
		"FROM qail_bench_payload " +
		"WHERE id <= $1::int " +
		"ORDER BY id"

	monsterCTESQL = "WITH base AS (" +
		"  SELECT id, visits, active, COALESCE(octet_length(optional_note), 0) AS note_len " +
		"  FROM qail_bench_payload " +
		"  WHERE id <= $1::int" +
		"), ranked AS (" +
		"  SELECT id, visits, note_len, " +
		"         row_number() OVER (ORDER BY visits DESC) AS rn, " +
		"         lag(visits, 1, 0) OVER (ORDER BY visits DESC) AS prev_visits " +
		"  FROM base" +
		"), bucketed AS (" +
		"  SELECT (id % 32) AS bucket, " +
		"         sum(visits) AS total_visits, " +
		"         max(note_len) AS max_note_len, " +
		"         sum(CASE WHEN active THEN 1 ELSE 0 END) AS active_count " +
		"  FROM base " +
		"  GROUP BY 1" +
		"), joined AS (" +
		"  SELECT r.id, r.visits, r.prev_visits, r.note_len, " +
		"         b.total_visits, b.max_note_len, b.active_count " +
		"  FROM ranked r " +
		"  JOIN bucketed b ON (r.id % 32) = b.bucket " +
		"  WHERE r.rn <= 256" +
		") " +
		"SELECT (" +
		"  COALESCE(sum(visits + prev_visits + note_len), 0)::bigint + " +
		"  COALESCE(max(total_visits), 0)::bigint + " +
		"  COALESCE(max(max_note_len), 0)::bigint + " +
		"  COALESCE(sum(active_count), 0)::bigint" +
		") AS total " +
		"FROM joined"

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
	largeRowsBatchSize   = 20
	largeRowsIterations  = 2
	manyParamsBatchSize  = 5_000
	manyParamsIterations = 5
	monsterCTEBatchSize  = 20
	monsterCTEIterations = 2

	fnvOffset = uint64(0xcbf29ce484222325)
	fnvPrime  = uint64(1099511628211)

	benchPayloadTargetRows = 20_000
	benchSetupLockSQL      = "SELECT pg_advisory_lock(60119029)"
	benchSetupUnlockSQL    = "SELECT pg_advisory_unlock(60119029)"
	createBenchPayloadSQL  = "CREATE TABLE IF NOT EXISTS qail_bench_payload (" +
		"id INTEGER PRIMARY KEY, " +
		"name TEXT NOT NULL, " +
		"bio TEXT NOT NULL, " +
		"region TEXT NOT NULL, " +
		"visits INTEGER NOT NULL, " +
		"active BOOLEAN NOT NULL, " +
		"ratio NUMERIC(12, 3) NOT NULL, " +
		"optional_note TEXT NULL" +
		")"
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
	name                 string
	sql                  string
	batchSize            int
	iterations           int
	latencySamples       int
	mode                 resultMode
	requiresBenchPayload bool
}

type statementMode int

const (
	statementModePrepared statementMode = iota
	statementModeUnprepared
)

func parseStatementMode(name string) (statementMode, error) {
	switch name {
	case "", "prepared", "prep":
		return statementModePrepared, nil
	case "unprepared", "uncached", "raw":
		return statementModeUnprepared, nil
	default:
		return statementModePrepared, fmt.Errorf("unknown statement mode %q (expected prepared or unprepared)", name)
	}
}

func (m statementMode) String() string {
	switch m {
	case statementModePrepared:
		return "prepared"
	case statementModeUnprepared:
		return "unprepared"
	default:
		return "unknown"
	}
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

type latencyResult struct {
	avgMs float64
	p50Ms float64
	p95Ms float64
	p99Ms float64
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: pgx_benchmark [--mode strict|once|single|pipeline|pool10|latency] [--workload literal|param|point|wide_rows|large_rows|many_params|monster_cte] [--stmt-mode prepared|unprepared] [--plain]\n")
	flag.PrintDefaults()
}

func envOverride(primary, fallback, defaultValue string) string {
	if value := os.Getenv(primary); value != "" {
		return value
	}
	if value := os.Getenv(fallback); value != "" {
		return value
	}
	return defaultValue
}

func benchmarkConnString() string {
	if url := os.Getenv("QAIL_BENCH_DATABASE_URL"); url != "" {
		return url
	}
	if url := os.Getenv("DATABASE_URL"); url != "" {
		return url
	}

	host := envOverride("QAIL_BENCH_HOST", "PGHOST", "127.0.0.1")
	port := envOverride("QAIL_BENCH_PORT", "PGPORT", "5432")
	user := envOverride("QAIL_BENCH_USER", "PGUSER", "orion")
	database := envOverride("QAIL_BENCH_DB", "PGDATABASE", "example_staging")
	password := envOverride("QAIL_BENCH_PASSWORD", "PGPASSWORD", "")
	sslmode := envOverride("QAIL_BENCH_SSLMODE", "PGSSLMODE", "disable")

	parts := []string{
		fmt.Sprintf("host=%s", host),
		fmt.Sprintf("port=%s", port),
		fmt.Sprintf("user=%s", user),
		fmt.Sprintf("dbname=%s", database),
		fmt.Sprintf("sslmode=%s", sslmode),
	}
	if password != "" {
		parts = append(parts, fmt.Sprintf("password=%s", password))
	}
	return strings.Join(parts, " ")
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
			name:                 "point",
			sql:                  sqlByID,
			batchSize:            pointBatchSize,
			iterations:           pointIterations,
			latencySamples:       2000,
			mode:                 resultModeCompleteOnly,
			requiresBenchPayload: false,
		}, nil
	case "wide_rows", "wide":
		return modeWorkload{
			name:                 "wide_rows",
			sql:                  wideRowsSQL,
			batchSize:            wideRowsBatchSize,
			iterations:           wideRowsIterations,
			latencySamples:       120,
			mode:                 resultModeWideRows,
			requiresBenchPayload: false,
		}, nil
	case "large_rows", "large":
		return modeWorkload{
			name:                 "large_rows",
			sql:                  largeRowsSQL,
			batchSize:            largeRowsBatchSize,
			iterations:           largeRowsIterations,
			latencySamples:       40,
			mode:                 resultModeWideRows,
			requiresBenchPayload: true,
		}, nil
	case "many_params", "params":
		return modeWorkload{
			name:                 "many_params",
			sql:                  manyParamsSQL,
			batchSize:            manyParamsBatchSize,
			iterations:           manyParamsIterations,
			latencySamples:       2000,
			mode:                 resultModeScalarInt,
			requiresBenchPayload: false,
		}, nil
	case "monster_cte", "cte", "server_heavy":
		return modeWorkload{
			name:                 "monster_cte",
			sql:                  monsterCTESQL,
			batchSize:            monsterCTEBatchSize,
			iterations:           monsterCTEIterations,
			latencySamples:       40,
			mode:                 resultModeScalarInt,
			requiresBenchPayload: true,
		}, nil
	default:
		return modeWorkload{}, fmt.Errorf("unknown workload %q (expected point, wide_rows, large_rows, many_params, or monster_cte)", name)
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
	case "large_rows":
		rowCounts := []string{"10000", "12000", "14000", "16000"}
		params := make([][][]byte, 0, spec.batchSize)
		for i := 0; i < spec.batchSize; i++ {
			params = append(params, [][]byte{[]byte(rowCounts[i%len(rowCounts)])})
		}
		return params
	case "monster_cte":
		rowCounts := []string{"8000", "12000", "16000", "20000"}
		params := make([][][]byte, 0, spec.batchSize)
		for i := 0; i < spec.batchSize; i++ {
			params = append(params, [][]byte{[]byte(rowCounts[i%len(rowCounts)])})
		}
		return params
	default:
		return nil
	}
}

func ensureBenchPayload(ctx context.Context, conn *pgx.Conn) error {
	if _, err := conn.Exec(ctx, benchSetupLockSQL); err != nil {
		return err
	}
	defer func() {
		_, _ = conn.Exec(ctx, benchSetupUnlockSQL)
	}()

	if _, err := conn.Exec(ctx, createBenchPayloadSQL); err != nil {
		return err
	}

	var currentRows int
	if err := conn.QueryRow(ctx, "SELECT COALESCE(MAX(id), 0) FROM qail_bench_payload").Scan(&currentRows); err != nil {
		return err
	}
	if currentRows < benchPayloadTargetRows {
		insertSQL := fmt.Sprintf(
			"INSERT INTO qail_bench_payload "+
				"(id, name, bio, region, visits, active, ratio, optional_note) "+
				"SELECT gs, "+
				"       ('harbor-' || gs)::text, "+
				"       repeat(md5(gs::text), 4), "+
				"       repeat(md5((gs * 17)::text), 3), "+
				"       (gs * 11), "+
				"       (gs %% 2 = 0), "+
				"       round((gs::numeric / 7.0), 3), "+
				"       CASE WHEN gs %% 5 = 0 THEN NULL ELSE repeat(md5((gs * 3)::text), 2) END "+
				"FROM generate_series(%d, %d) AS gs "+
				"ON CONFLICT (id) DO NOTHING",
			currentRows+1,
			benchPayloadTargetRows,
		)
		if _, err := conn.Exec(ctx, insertSQL); err != nil {
			return err
		}
		_, _ = conn.Exec(ctx, "ANALYZE qail_bench_payload")
	}
	return nil
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

func runPipelineOnceUnprepared(p *pgconn.Pipeline, sql string, params [][][]byte, mode resultMode) (batchStats, error) {
	for _, paramSet := range params {
		p.SendQueryParams(sql, paramSet, nil, nil, nil)
	}
	if err := p.Sync(); err != nil {
		return batchStats{}, err
	}

	expected := len(params)
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
	setup func(context.Context, *pgx.Conn) error,
) (benchmarkResult, error) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, benchmarkConnString())
	if err != nil {
		return benchmarkResult{}, err
	}
	defer conn.Close(ctx)
	if setup != nil {
		if err := setup(ctx, conn); err != nil {
			return benchmarkResult{}, err
		}
	}

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

func runUnpreparedPipelineModeBenchmark(
	spec modeWorkload,
	iterations int,
	setup func(context.Context, *pgx.Conn) error,
) (benchmarkResult, error) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, benchmarkConnString())
	if err != nil {
		return benchmarkResult{}, err
	}
	defer conn.Close(ctx)
	if setup != nil {
		if err := setup(ctx, conn); err != nil {
			return benchmarkResult{}, err
		}
	}

	p := conn.PgConn().StartPipeline(ctx)
	defer p.Close()

	params := buildModeParamBatch(spec)
	warmup, err := runPipelineOnceUnprepared(p, spec.sql, params, spec.mode)
	if err != nil {
		return benchmarkResult{}, err
	}
	if warmup.completed != len(params) {
		return benchmarkResult{}, fmt.Errorf("warmup completed %d queries, expected %d", warmup.completed, len(params))
	}

	total := time.Duration(0)
	aggregate := batchStats{}
	for i := 0; i < iterations; i++ {
		start := time.Now()
		stats, err := runPipelineOnceUnprepared(p, spec.sql, params, spec.mode)
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

func runPreparedPipelineBenchmark(calls []preparedCall, templates map[string]string, orderedNames []string) (float64, error) {
	result, err := runPreparedPipelineModeBenchmark(calls, templates, orderedNames, resultModeCompleteOnly, 5, nil)
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

func runSingleUnpreparedOnce(conn *pgconn.PgConn, sql string, params [][][]byte, mode resultMode) (batchStats, error) {
	ctx := context.Background()
	stats := batchStats{}

	for _, paramSet := range params {
		rr := conn.ExecParams(ctx, sql, paramSet, nil, nil, nil)
		readerStats, err := consumeResultReader(rr, mode)
		if err != nil {
			return batchStats{}, err
		}
		stats.add(readerStats)
	}

	return stats, nil
}

func runSingleMode(spec modeWorkload, stmtMode statementMode) (benchmarkResult, error) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, benchmarkConnString())
	if err != nil {
		return benchmarkResult{}, err
	}
	defer conn.Close(ctx)
	if spec.requiresBenchPayload {
		if err := ensureBenchPayload(ctx, conn); err != nil {
			return benchmarkResult{}, err
		}
	}

	pgConn := conn.PgConn()
	params := buildModeParamBatch(spec)
	if stmtMode == statementModePrepared {
		if _, err := pgConn.Prepare(ctx, "single_stmt", spec.sql, nil); err != nil {
			return benchmarkResult{}, err
		}
	}

	var warmup batchStats
	switch stmtMode {
	case statementModePrepared:
		warmup, err = runSinglePreparedOnce(pgConn, "single_stmt", params, spec.mode)
	case statementModeUnprepared:
		warmup, err = runSingleUnpreparedOnce(pgConn, spec.sql, params, spec.mode)
	}
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
		var stats batchStats
		switch stmtMode {
		case statementModePrepared:
			stats, err = runSinglePreparedOnce(pgConn, "single_stmt", params, spec.mode)
		case statementModeUnprepared:
			stats, err = runSingleUnpreparedOnce(pgConn, spec.sql, params, spec.mode)
		}
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

func runPipelineMode(spec modeWorkload, stmtMode statementMode) (benchmarkResult, error) {
	var setup func(context.Context, *pgx.Conn) error
	if spec.requiresBenchPayload {
		setup = ensureBenchPayload
	}
	switch stmtMode {
	case statementModePrepared:
		calls, templates, ordered, _ := buildModeCalls(spec)
		return runPreparedPipelineModeBenchmark(calls, templates, ordered, spec.mode, spec.iterations, setup)
	case statementModeUnprepared:
		return runUnpreparedPipelineModeBenchmark(spec, spec.iterations, setup)
	default:
		return benchmarkResult{}, fmt.Errorf("unsupported statement mode %v", stmtMode)
	}
}

func runPool10Mode(spec modeWorkload, stmtMode statementMode) (benchmarkResult, error) {
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(benchmarkConnString())
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
	if spec.requiresBenchPayload {
		conn, err := pgx.Connect(ctx, benchmarkConnString())
		if err != nil {
			return benchmarkResult{}, err
		}
		if err := ensureBenchPayload(ctx, conn); err != nil {
			conn.Close(ctx)
			return benchmarkResult{}, err
		}
		conn.Close(ctx)
	}

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
			if stmtMode == statementModePrepared {
				if _, err := pgConn.Prepare(ctx, stmtName, spec.sql, nil); err != nil {
					readyCh <- struct{}{}
					errCh <- err
					return
				}
			}

			var warmup batchStats
			switch stmtMode {
			case statementModePrepared:
				warmup, err = runSinglePreparedOnce(pgConn, stmtName, vals, spec.mode)
			case statementModeUnprepared:
				warmup, err = runSingleUnpreparedOnce(pgConn, spec.sql, vals, spec.mode)
			}
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
				var stats batchStats
				switch stmtMode {
				case statementModePrepared:
					stats, err = runSinglePreparedOnce(pgConn, stmtName, vals, spec.mode)
				case statementModeUnprepared:
					stats, err = runSingleUnpreparedOnce(pgConn, spec.sql, vals, spec.mode)
				}
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

func runLatencyMode(spec modeWorkload, stmtMode statementMode) (latencyResult, error) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, benchmarkConnString())
	if err != nil {
		return latencyResult{}, err
	}
	defer conn.Close(ctx)
	if spec.requiresBenchPayload {
		if err := ensureBenchPayload(ctx, conn); err != nil {
			return latencyResult{}, err
		}
	}

	pgConn := conn.PgConn()
	params := buildModeParamBatch(spec)
	if stmtMode == statementModePrepared {
		if _, err := pgConn.Prepare(ctx, "latency_stmt", spec.sql, nil); err != nil {
			return latencyResult{}, err
		}
	}

	warmupCount := spec.latencySamples
	if warmupCount > 20 {
		warmupCount = 20
	}
	for i := 0; i < warmupCount; i++ {
		paramSet := params[i%len(params)]
		switch stmtMode {
		case statementModePrepared:
			_, err = runSinglePreparedOnce(pgConn, "latency_stmt", [][][]byte{paramSet}, spec.mode)
		case statementModeUnprepared:
			_, err = runSingleUnpreparedOnce(pgConn, spec.sql, [][][]byte{paramSet}, spec.mode)
		}
		if err != nil {
			return latencyResult{}, err
		}
	}

	samples := make([]time.Duration, 0, spec.latencySamples)
	total := time.Duration(0)
	for i := 0; i < spec.latencySamples; i++ {
		paramSet := params[i%len(params)]
		start := time.Now()
		var stats batchStats
		switch stmtMode {
		case statementModePrepared:
			stats, err = runSinglePreparedOnce(pgConn, "latency_stmt", [][][]byte{paramSet}, spec.mode)
		case statementModeUnprepared:
			stats, err = runSingleUnpreparedOnce(pgConn, spec.sql, [][][]byte{paramSet}, spec.mode)
		}
		elapsed := time.Since(start)
		if err != nil {
			return latencyResult{}, err
		}
		if stats.completed != 1 {
			return latencyResult{}, fmt.Errorf("latency sample completed %d queries, expected 1", stats.completed)
		}
		total += elapsed
		samples = append(samples, elapsed)
	}

	sort.Slice(samples, func(i, j int) bool {
		return samples[i] < samples[j]
	})
	p50 := samples[len(samples)/2]
	p95Idx := int(float64(len(samples))*0.95 + 0.999999999)
	if p95Idx < 1 {
		p95Idx = 1
	}
	if p95Idx > len(samples) {
		p95Idx = len(samples)
	}
	p99Idx := int(float64(len(samples))*0.99 + 0.999999999)
	if p99Idx < 1 {
		p99Idx = 1
	}
	if p99Idx > len(samples) {
		p99Idx = len(samples)
	}

	return latencyResult{
		avgMs: total.Seconds() * 1000.0 / float64(len(samples)),
		p50Ms: p50.Seconds() * 1000.0,
		p95Ms: samples[p95Idx-1].Seconds() * 1000.0,
		p99Ms: samples[p99Idx-1].Seconds() * 1000.0,
	}, nil
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
	mode := flag.String("mode", "strict", "benchmark mode: strict, once, single, pipeline, pool10, or latency")
	workload := flag.String("workload", "", "workload name: strict/once use literal|param; single/pipeline/pool10/latency use point|wide_rows|large_rows|many_params|monster_cte")
	stmtModeName := flag.String("stmt-mode", "prepared", "statement mode for single/pipeline/pool10/latency: prepared or unprepared")
	plain := flag.Bool("plain", false, "print only numeric q/s in single-run modes")
	flag.Usage = usage
	flag.Parse()

	switch *mode {
	case "single", "pipeline", "pool10", "latency":
		stmtMode, err := parseStatementMode(*stmtModeName)
		if err != nil {
			panic(err)
		}
		workloadName := *workload
		if workloadName == "" {
			workloadName = "point"
		}

		spec, err := modeWorkloadFromName(workloadName)
		if err != nil {
			panic(err)
		}

		if *mode == "latency" {
			result, err := runLatencyMode(spec, stmtMode)
			if err != nil {
				panic(err)
			}
			if *plain {
				fmt.Printf("%.6f,%.6f,%.6f,%.6f\n", result.p50Ms, result.p95Ms, result.p99Ms, result.avgMs)
			} else {
				fmt.Printf("%s/%s/%s: p50=%.3f ms | p95=%.3f ms | p99=%.3f ms | avg=%.3f ms\n", *mode, stmtMode.String(), spec.name, result.p50Ms, result.p95Ms, result.p99Ms, result.avgMs)
			}
			return
		}

		var result benchmarkResult
		switch *mode {
		case "single":
			result, err = runSingleMode(spec, stmtMode)
		case "pipeline":
			result, err = runPipelineMode(spec, stmtMode)
		case "pool10":
			result, err = runPool10Mode(spec, stmtMode)
		}
		if err != nil {
			panic(err)
		}

		printModeResult(fmt.Sprintf("%s/%s/%s", *mode, stmtMode.String(), spec.name), result, *plain, spec.mode)
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
		panic(fmt.Errorf("unknown mode %q (expected strict, once, single, pipeline, pool10, or latency)", *mode))
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

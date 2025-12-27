<?php
/**
 * QAIL-PHP TRUE PIPELINE BENCHMARK
 * 
 * Uses Rust-managed connection for real pipelining.
 * Expected: 10x faster than PDO (~300K q/s vs 30K q/s)
 */

declare(strict_types=1);

$ffi = FFI::cdef("
    void* qail_connect(const char* host, uint16_t port, const char* user, const char* database);
    void qail_disconnect(void* conn);
    void* qail_prepare(void* conn, const char* sql);
    void qail_prepared_free(void* stmt);
    int64_t qail_pipeline_exec_limits(void* conn, void* stmt, const int64_t* limits, size_t count);
", __DIR__ . "/../../target/release/libqail_php.dylib");

$host = getenv('PG_HOST') ?: '127.0.0.1';
$port = (int)(getenv('PG_PORT') ?: 5432);
$user = getenv('PG_USER') ?: 'orion';
$database = getenv('PG_DATABASE') ?: 'postgres';

const TOTAL_QUERIES = 100_000;
const BATCH_SIZE = 100;
const BATCHES = TOTAL_QUERIES / BATCH_SIZE;

echo "╔════════════════════════════════════════════════════════════╗\n";
echo "║  QAIL-PHP TRUE PIPELINE BENCHMARK                          ║\n";
echo "╚════════════════════════════════════════════════════════════╝\n\n";

// === TEST 1: Raw PDO (baseline) ===
echo "📊 [1/2] PDO Baseline...\n";

$dsn = "pgsql:host=$host;port=$port;dbname=$database";
$pdo = new PDO($dsn, $user, '', [
    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
    PDO::ATTR_EMULATE_PREPARES => false,
]);

$stmt = $pdo->prepare('SELECT id, name FROM harbors LIMIT ?');

$start = microtime(true);
for ($i = 0; $i < TOTAL_QUERIES; $i++) {
    $stmt->execute([($i % 10) + 1]);
    $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
}
$pdoTime = microtime(true) - $start;
$pdoQps = TOTAL_QUERIES / $pdoTime;

echo "   " . number_format($pdoQps, 0) . " q/s (" . round($pdoTime, 2) . "s)\n\n";

// === TEST 2: QAIL Pipeline ===
echo "📊 [2/2] QAIL Pipeline (Rust)...\n";

$conn = $ffi->qail_connect($host, $port, $user, $database);
if (FFI::isNull($conn)) {
    echo "   ❌ Connection failed\n";
    exit(1);
}

$qailStmt = $ffi->qail_prepare($conn, 'SELECT id, name FROM harbors LIMIT $1');
if (FFI::isNull($qailStmt)) {
    echo "   ❌ Prepare failed\n";
    $ffi->qail_disconnect($conn);
    exit(1);
}

// Build limits array
$limits = $ffi->new("int64_t[" . BATCH_SIZE . "]");
for ($i = 0; $i < BATCH_SIZE; $i++) {
    $limits[$i] = ($i % 10) + 1;
}

// Run benchmark
$start = microtime(true);
$completed = 0;
for ($batch = 0; $batch < BATCHES; $batch++) {
    $result = $ffi->qail_pipeline_exec_limits($conn, $qailStmt, $limits, BATCH_SIZE);
    if ($result < 0) {
        echo "   ❌ Pipeline failed at batch $batch\n";
        break;
    }
    $completed += $result;
}
$qailTime = microtime(true) - $start;
$qailQps = $completed > 0 ? $completed / $qailTime : 0;

echo "   " . number_format($qailQps, 0) . " q/s (" . round($qailTime, 2) . "s)\n\n";

// Cleanup
$ffi->qail_prepared_free($qailStmt);
$ffi->qail_disconnect($conn);

// === RESULTS ===
$speedup = $pdoQps > 0 ? $qailQps / $pdoQps : 0;

echo "📈 RESULTS:\n";
echo "┌───────────────────────────────────────┐\n";
printf("│ PDO:           %'11s q/s       │\n", number_format($pdoQps, 0));
printf("│ QAIL Pipeline: %'11s q/s       │\n", number_format($qailQps, 0));
echo "├───────────────────────────────────────┤\n";
printf("│ Speedup:       %'.1fx                 │\n", $speedup);
echo "└───────────────────────────────────────┘\n";

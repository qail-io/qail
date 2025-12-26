<?php
/**
 * QAIL-PHP Benchmark
 * 
 * Tests encoding performance of QAIL-PHP vs raw PDO
 * 
 * Run:
 *   php /Users/orion/qail.rs/qail-php/php/bench.php
 */

declare(strict_types=1);

require_once __DIR__ . '/Qail.php';

// Configuration
$libPath = __DIR__ . '/../../target/release/libqail_php.dylib';
$host = getenv('PG_HOST') ?: '127.0.0.1';
$port = getenv('PG_PORT') ?: '5432';
$user = getenv('PG_USER') ?: 'orion';
$database = getenv('PG_DATABASE') ?: 'postgres';

const ITERATIONS = 100000;
const BATCH_SIZE = 1000;
const BATCHES = 100;

echo "🏁 QAIL-PHP BENCHMARK\n";
echo "=====================\n";
echo "Iterations: " . ITERATIONS . "\n";
echo "Batch size: " . BATCH_SIZE . "\n\n";

// Initialize QAIL
$qail = new Qail($libPath);
echo "✅ QAIL version: " . $qail->version() . "\n\n";

// ========== Test 1: Pure encoding speed ==========
echo "📊 Test 1: Pure Encoding Speed (QAIL FFI)\n";

$start = microtime(true);
for ($i = 0; $i < ITERATIONS; $i++) {
    $bytes = $qail->encodeSelect('harbors', 'id,name', ($i % 10) + 1);
}
$elapsed = microtime(true) - $start;
$encodeOps = ITERATIONS / $elapsed;
$usPerOp = ($elapsed * 1_000_000) / ITERATIONS;

echo "   " . number_format(ITERATIONS) . " encodes in " . number_format($elapsed, 2) . "s\n";
echo "   " . number_format($encodeOps, 0) . " ops/sec\n";
echo "   " . number_format($usPerOp, 2) . " µs/op\n\n";

// ========== Test 2: Batch encoding ==========
echo "📊 Test 2: Batch Encoding (" . BATCH_SIZE . " queries per batch)\n";

$limits = array_map(fn($i) => ($i % 10) + 1, range(0, BATCH_SIZE - 1));

$start = microtime(true);
for ($batch = 0; $batch < BATCHES; $batch++) {
    $bytes = $qail->encodeBatch('harbors', 'id,name', $limits);
}
$elapsed = microtime(true) - $start;
$totalQueries = BATCH_SIZE * BATCHES;
$batchOps = $totalQueries / $elapsed;

echo "   " . number_format($totalQueries) . " queries in " . number_format($elapsed, 2) . "s\n";
echo "   " . number_format($batchOps, 0) . " ops/sec\n\n";

// ========== Test 3: QAIL text transpile ==========
echo "📊 Test 3: QAIL Text Transpile (QAIL → SQL)\n";

$start = microtime(true);
for ($i = 0; $i < ITERATIONS; $i++) {
    $sql = $qail->transpile('get harbors');
}
$elapsed = microtime(true) - $start;
$transpileOps = ITERATIONS / $elapsed;

echo "   " . number_format(ITERATIONS) . " transpiles in " . number_format($elapsed, 2) . "s\n";
echo "   " . number_format($transpileOps, 0) . " ops/sec\n\n";

// ========== Summary ==========
echo "📈 SUMMARY:\n";
echo "┌────────────────────────────────────────┐\n";
printf("│ Encode:     %'11s ops/sec         │\n", number_format($encodeOps, 0));
printf("│ Batch:      %'11s q/s             │\n", number_format($batchOps, 0));
printf("│ Transpile:  %'11s ops/sec         │\n", number_format($transpileOps, 0));
echo "├────────────────────────────────────────┤\n";
echo "│ For context:                           │\n";
echo "│ - WASM:        806,000 ops/sec         │\n";
echo "│ - Go CGO:      126,000 q/s (with I/O)  │\n";
echo "│ - PHP is encoding ONLY (no I/O)        │\n";
echo "└────────────────────────────────────────┘\n";

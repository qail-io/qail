<?php
/**
 * QAIL PHP Extension vs FFI Benchmark
 * 
 * Run: php -d extension=/Users/orion/qail.rs/qail-php/ext/qail/modules/qail.so bench_ext.php
 */

declare(strict_types=1);

const ITERATIONS = 100000;

echo "🏁 QAIL-PHP EXTENSION vs FFI BENCHMARK\n";
echo "======================================\n";
echo "Iterations: " . ITERATIONS . "\n\n";

// ========== Test 1: Extension (native) ==========
echo "📊 [1/2] QAIL Extension (native C)...\n";

$start = microtime(true);
for ($i = 0; $i < ITERATIONS; $i++) {
    $bytes = qail_encode_select('harbors', 'id,name', ($i % 10) + 1);
}
$elapsed = microtime(true) - $start;
$extOps = ITERATIONS / $elapsed;
$extUs = ($elapsed * 1_000_000) / ITERATIONS;

echo "   " . number_format(ITERATIONS) . " encodes in " . number_format($elapsed, 2) . "s\n";
echo "   " . number_format($extOps, 0) . " ops/sec\n";
echo "   " . number_format($extUs, 2) . " µs/op\n\n";

// ========== Test 2: FFI (for comparison) ==========
echo "📊 [2/2] QAIL FFI (PHP FFI)...\n";

require_once '/Users/orion/qail.rs/qail-php/php/Qail.php';
$qail = new Qail('/Users/orion/qail.rs/target/release/libqail_php.dylib');

$start = microtime(true);
for ($i = 0; $i < ITERATIONS; $i++) {
    $bytes = $qail->encodeSelect('harbors', 'id,name', ($i % 10) + 1);
}
$elapsed = microtime(true) - $start;
$ffiOps = ITERATIONS / $elapsed;
$ffiUs = ($elapsed * 1_000_000) / ITERATIONS;

echo "   " . number_format(ITERATIONS) . " encodes in " . number_format($elapsed, 2) . "s\n";
echo "   " . number_format($ffiOps, 0) . " ops/sec\n";
echo "   " . number_format($ffiUs, 2) . " µs/op\n\n";

// ========== Summary ==========
echo "📈 RESULTS:\n";
echo "┌────────────────────────────────────────┐\n";
printf("│ Extension: %'10s ops/sec          │\n", number_format($extOps, 0));
printf("│ FFI:       %'10s ops/sec          │\n", number_format($ffiOps, 0));
echo "├────────────────────────────────────────┤\n";
$speedup = $extOps / $ffiOps;
printf("│ Extension is %.1fx FASTER than FFI  │\n", $speedup);
echo "├────────────────────────────────────────┤\n";
echo "│ For context:                           │\n";
echo "│ - Eloquent: ~10,000 q/s (with I/O)     │\n";
echo "│ - Raw PDO:  ~33,000 q/s (with I/O)     │\n";
echo "└────────────────────────────────────────┘\n";

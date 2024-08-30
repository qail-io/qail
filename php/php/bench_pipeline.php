<?php
/**
 * QAIL PHP Extension Pipeline Benchmark
 * 
 * Tests batch/pipeline encoding vs individual queries
 * 
 * Run: php -d extension=/path/to/qail.so bench_pipeline.php
 */

declare(strict_types=1);

const BATCH_SIZE = 1000;
const BATCHES = 100;
const TOTAL = BATCH_SIZE * BATCHES;

echo "🏁 QAIL-PHP PIPELINE BENCHMARK\n";
echo "==============================\n";
echo "Batch size: " . BATCH_SIZE . "\n";
echo "Batches: " . BATCHES . "\n";
echo "Total queries: " . TOTAL . "\n\n";

// Pre-build limits array
$limits = [];
for ($i = 0; $i < BATCH_SIZE; $i++) {
    $limits[] = ($i % 10) + 1;
}

// ========== Test 1: Individual queries ==========
echo "📊 [1/2] Individual Encoding (one at a time)...\n";

$start = microtime(true);
for ($batch = 0; $batch < BATCHES; $batch++) {
    for ($i = 0; $i < BATCH_SIZE; $i++) {
        $bytes = qail_encode_select('harbors', 'id,name', $limits[$i]);
    }
    if ($batch % 20 == 0) echo "   Batch $batch/$BATCHES\n";
}
$elapsed = microtime(true) - $start;
$individualOps = TOTAL / $elapsed;

echo "   " . number_format(TOTAL) . " encodes in " . number_format($elapsed, 2) . "s\n";
echo "   " . number_format($individualOps, 0) . " ops/sec\n\n";

// ========== Test 2: Batch/Pipeline encoding ==========
echo "📊 [2/2] Pipeline Encoding (batch of $BATCH_SIZE)...\n";

$start = microtime(true);
for ($batch = 0; $batch < BATCHES; $batch++) {
    $bytes = qail_encode_batch('harbors', 'id,name', $limits);
    if ($batch % 20 == 0) echo "   Batch $batch/$BATCHES\n";
}
$elapsed = microtime(true) - $start;
$pipelineOps = TOTAL / $elapsed;

echo "   " . number_format(TOTAL) . " encodes in " . number_format($elapsed, 2) . "s\n";
echo "   " . number_format($pipelineOps, 0) . " q/s (batched)\n\n";

// ========== Summary ==========
echo "📈 RESULTS:\n";
echo "┌────────────────────────────────────────┐\n";
printf("│ Individual:  %'10s q/s            │\n", number_format($individualOps, 0));
printf("│ Pipeline:    %'10s q/s            │\n", number_format($pipelineOps, 0));
echo "├────────────────────────────────────────┤\n";
$speedup = $pipelineOps / $individualOps;
printf("│ Pipeline is %.1fx FASTER             │\n", $speedup);
echo "├────────────────────────────────────────┤\n";
echo "│ For context:                           │\n";
echo "│ - With I/O, pipeline reduces RTTs      │\n";
echo "│ - 1000 queries = 1 network round-trip  │\n";
echo "└────────────────────────────────────────┘\n";

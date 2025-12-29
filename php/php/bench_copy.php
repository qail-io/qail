<?php
/**
 * QAIL Streaming COPY Benchmark
 * 
 * Compares:
 * - PDO pgsqlCopyFromArray (380K rows/s baseline)
 * - QAIL streaming COPY (target: 600K+ rows/s)
 */

declare(strict_types=1);

$ffi = FFI::cdef("
    void* qail_connect(const char* host, uint16_t port, const char* user, const char* database);
    void qail_disconnect(void* conn);
    
    void* qail_copy_start(void* conn, const char* table, const char* columns);
    int32_t qail_copy_row_4(void* stream, const char* c0, const char* c1, const char* c2, const char* c3);
    int32_t qail_copy_row_6(void* stream, const char* c0, const char* c1, const char* c2, const char* c3, const char* c4, const char* c5);
    int64_t qail_copy_end(void* stream);
    void qail_copy_cancel(void* stream);
", __DIR__ . "/../../target/release/libqail_php.dylib");

$host = '127.0.0.1';
$port = 5432;
$user = 'orion';
$database = 'postgres';

// MySQL source
$mysql = new PDO("mysql:host=127.0.0.1;dbname=ecommerce", "root", "", [
    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
]);

echo "╔════════════════════════════════════════════════════════════╗\n";
echo "║  PDO COPY vs QAIL Streaming COPY Benchmark                 ║\n";
echo "╚════════════════════════════════════════════════════════════╝\n\n";

// Count orders
$orderCount = (int)$mysql->query("SELECT COUNT(*) FROM orders")->fetchColumn();
echo "📊 Orders table: " . number_format($orderCount) . " rows\n\n";

// ============== TEST 1: PDO COPY ==============
echo "🔄 [1/2] PDO pgsqlCopyFromArray...\n";

$pg = new PDO("pgsql:host=$host;port=$port;dbname=$database", $user, "", [
    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
]);
$pg->exec("TRUNCATE orders CASCADE");

// Helper to convert rows to TSV
function rowsToTsv(array $rows): array {
    return array_map(fn($r) => implode("\t", array_map(fn($v) => $v ?? '\N', $r)), $rows);
}

$start = microtime(true);
$batchSize = 100000;
$offset = 0;
while ($offset < $orderCount) {
    $rows = $mysql->query("SELECT id, user_id, product_id, quantity, total, created_at FROM orders LIMIT $batchSize OFFSET $offset")->fetchAll(PDO::FETCH_NUM);
    if (empty($rows)) break;
    $tsv = rowsToTsv($rows);
    $pg->pgsqlCopyFromArray("orders", $tsv, "\t", "\\N", "id,user_id,product_id,quantity,total,created_at");
    $offset += $batchSize;
}
$pdoTime = microtime(true) - $start;
$pdoRate = $orderCount / $pdoTime;

echo "   " . number_format($pdoRate, 0) . " rows/s (" . round($pdoTime, 2) . "s)\n\n";

// ============== TEST 2: QAIL Streaming COPY ==============
echo "🔄 [2/2] QAIL Streaming COPY...\n";

$pg->exec("TRUNCATE orders CASCADE");

$conn = $ffi->qail_connect($host, $port, $user, $database);
if (FFI::isNull($conn)) {
    echo "   ❌ Connection failed\n";
    exit(1);
}

$start = microtime(true);
$copy = $ffi->qail_copy_start($conn, "orders", "id,user_id,product_id,quantity,total,created_at");
if (FFI::isNull($copy)) {
    echo "   ❌ COPY start failed\n";
    $ffi->qail_disconnect($conn);
    exit(1);
}

// Stream directly from MySQL to Rust buffer
$offset = 0;
while ($offset < $orderCount) {
    $stmt = $mysql->query("SELECT id, user_id, product_id, quantity, total, created_at FROM orders LIMIT $batchSize OFFSET $offset");
    while ($row = $stmt->fetch(PDO::FETCH_NUM)) {
        $ffi->qail_copy_row_6(
            $copy,
            (string)$row[0], // id
            (string)$row[1], // user_id
            (string)$row[2], // product_id
            (string)$row[3], // quantity
            (string)$row[4], // total
            $row[5] ?? '\N'  // created_at
        );
    }
    $offset += $batchSize;
}

$inserted = $ffi->qail_copy_end($copy);
$qailTime = microtime(true) - $start;
$qailRate = $inserted / $qailTime;

echo "   " . number_format($qailRate, 0) . " rows/s (" . round($qailTime, 2) . "s)\n";
echo "   Rows inserted: " . number_format($inserted) . "\n\n";

$ffi->qail_disconnect($conn);

// ============== RESULTS ==============
$speedup = $qailRate / $pdoRate;

echo "📈 RESULTS:\n";
echo "┌───────────────────────────────────────┐\n";
printf("│ PDO COPY:     %'12s rows/s     │\n", number_format($pdoRate, 0));
printf("│ QAIL COPY:    %'12s rows/s     │\n", number_format($qailRate, 0));
echo "├───────────────────────────────────────┤\n";
printf("│ Speedup:      %'.2fx                  │\n", $speedup);
echo "└───────────────────────────────────────┘\n";

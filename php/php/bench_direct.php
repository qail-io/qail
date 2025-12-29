<?php
/**
 * QAIL Direct MySQL→PostgreSQL Migration Benchmark
 * 
 * Compares:
 * - PHP Loop: MySQL read → PHP → PG COPY (~330K rows/s)
 * - QAIL Direct: MySQL → Rust → PG COPY (target: 600K+ rows/s)
 */

declare(strict_types=1);

$ffi = FFI::cdef("
    void* qail_connect(const char* host, uint16_t port, const char* user, const char* database);
    void qail_disconnect(void* conn);
    int64_t qail_mysql_to_pg(
        const char* mysql_host, uint16_t mysql_port,
        const char* mysql_user, const char* mysql_pass, const char* mysql_db,
        void* pg_conn,
        const char* sql,
        const char* pg_table,
        const char* pg_columns
    );
", __DIR__ . "/../../target/release/libqail_php.dylib");

$mysqlHost = '127.0.0.1';
$mysqlPort = 3306;
$mysqlUser = 'root';
$mysqlPass = '';
$mysqlDb = 'ecommerce';

$pgHost = '127.0.0.1';
$pgPort = 5432;
$pgUser = 'orion';
$pgDb = 'postgres';

echo "╔════════════════════════════════════════════════════════════╗\n";
echo "║  QAIL Direct MySQL → PostgreSQL Migration                  ║\n";
echo "║  Rust-to-Rust pipeline (no PHP loop)                       ║\n";
echo "╚════════════════════════════════════════════════════════════╝\n\n";

// Count rows
$mysql = new PDO("mysql:host=$mysqlHost;dbname=$mysqlDb", $mysqlUser, $mysqlPass);
$orderCount = (int)$mysql->query("SELECT COUNT(*) FROM orders")->fetchColumn();
echo "📊 Orders: " . number_format($orderCount) . " rows\n\n";

// Connect to PostgreSQL
$pg = new PDO("pgsql:host=$pgHost;port=$pgPort;dbname=$pgDb", $pgUser);
$pg->exec("TRUNCATE orders CASCADE");

$pgConn = $ffi->qail_connect($pgHost, $pgPort, $pgUser, $pgDb);
if (FFI::isNull($pgConn)) {
    echo "❌ PostgreSQL connection failed\n";
    exit(1);
}

echo "🔄 Migrating with QAIL Direct (Rust-to-Rust)...\n";
$start = microtime(true);

$rows = $ffi->qail_mysql_to_pg(
    $mysqlHost, $mysqlPort, $mysqlUser, $mysqlPass, $mysqlDb,
    $pgConn,
    "SELECT id, user_id, product_id, quantity, total, created_at FROM orders",
    "orders",
    "id,user_id,product_id,quantity,total,created_at"
);

$time = microtime(true) - $start;
$rate = $rows > 0 ? $rows / $time : 0;

$ffi->qail_disconnect($pgConn);

if ($rows < 0) {
    echo "❌ Migration failed\n";
    exit(1);
}

echo "   Rows migrated: " . number_format($rows) . "\n";
echo "   Time: " . round($time, 2) . "s\n";
echo "   Rate: " . number_format($rate, 0) . " rows/s\n\n";

// Verify
$pgCount = (int)$pg->query("SELECT COUNT(*) FROM orders")->fetchColumn();
echo "✅ Verification: MySQL=$orderCount, PG=$pgCount " . ($orderCount === $pgCount ? "✓" : "✗") . "\n\n";

echo "📈 RESULTS:\n";
echo "┌───────────────────────────────────────┐\n";
printf("│ QAIL Direct:  %'12s rows/s     │\n", number_format($rate, 0));
echo "│ (vs PHP Loop: ~330K rows/s)          │\n";
echo "├───────────────────────────────────────┤\n";
printf("│ Speedup:      ~%.1fx                  │\n", $rate / 330000);
echo "└───────────────────────────────────────┘\n";

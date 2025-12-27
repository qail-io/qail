<?php
/**
 * MySQL to PostgreSQL Migration Benchmark
 * 
 * Reads from MySQL via PDO, writes to PostgreSQL via COPY
 */

declare(strict_types=1);

echo "╔════════════════════════════════════════════════════════════╗\n";
echo "║  MySQL → PostgreSQL Migration Benchmark                    ║\n";
echo "╚════════════════════════════════════════════════════════════╝\n\n";

// MySQL connection
$mysql = new PDO("mysql:host=127.0.0.1;dbname=ecommerce", "root", "", [
    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
]);

// PostgreSQL connection
$pg = new PDO("pgsql:host=127.0.0.1;dbname=postgres", "orion", "", [
    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
]);

// Get row counts
$mysqlUsers = (int)$mysql->query("SELECT COUNT(*) FROM users")->fetchColumn();
$mysqlProducts = (int)$mysql->query("SELECT COUNT(*) FROM products")->fetchColumn();
$mysqlOrders = (int)$mysql->query("SELECT COUNT(*) FROM orders")->fetchColumn();

echo "📊 MySQL Source:\n";
echo "   Users:    " . number_format($mysqlUsers) . " rows\n";
echo "   Products: " . number_format($mysqlProducts) . " rows\n";
echo "   Orders:   " . number_format($mysqlOrders) . " rows\n";
echo "   Total:    " . number_format($mysqlUsers + $mysqlProducts + $mysqlOrders) . " rows (~38MB)\n\n";

// Helper: convert rows to TSV lines for COPY
function rowsToTsv(array $rows): array {
    return array_map(function($row) {
        return implode("\t", array_map(function($v) {
            return $v === null ? '\N' : str_replace(["\t", "\n"], [" ", " "], (string)$v);
        }, $row));
    }, $rows);
}

// ============== MIGRATE USERS ==============
echo "🔄 Migrating users...\n";
$pg->exec("TRUNCATE users CASCADE");

$start = microtime(true);
$rows = $mysql->query("SELECT id, name, email, created_at FROM users")->fetchAll(PDO::FETCH_NUM);
$readTime = microtime(true) - $start;

$start = microtime(true);
$tsv = rowsToTsv($rows);
$pg->pgsqlCopyFromArray("users", $tsv, "\t", "\\N", "id,name,email,created_at");
$writeTime = microtime(true) - $start;

$usersReadRate = $mysqlUsers / $readTime;
$usersWriteRate = $mysqlUsers / $writeTime;
printf("   MySQL read:  %s rows/s (%.2fs)\n", number_format($usersReadRate, 0), $readTime);
printf("   PG COPY:     %s rows/s (%.2fs)\n\n", number_format($usersWriteRate, 0), $writeTime);

// ============== MIGRATE PRODUCTS ==============
echo "🔄 Migrating products...\n";
$pg->exec("TRUNCATE products CASCADE");

$start = microtime(true);
$rows = $mysql->query("SELECT id, name, price, description, stock, created_at FROM products")->fetchAll(PDO::FETCH_NUM);
$readTime = microtime(true) - $start;

$start = microtime(true);
$tsv = rowsToTsv($rows);
$pg->pgsqlCopyFromArray("products", $tsv, "\t", "\\N", "id,name,price,description,stock,created_at");
$writeTime = microtime(true) - $start;

$prodsReadRate = $mysqlProducts / $readTime;
$prodsWriteRate = $mysqlProducts / $writeTime;
printf("   MySQL read:  %s rows/s (%.2fs)\n", number_format($prodsReadRate, 0), $readTime);
printf("   PG COPY:     %s rows/s (%.2fs)\n\n", number_format($prodsWriteRate, 0), $writeTime);

// ============== MIGRATE ORDERS ==============
echo "🔄 Migrating orders (500K rows)...\n";
$pg->exec("TRUNCATE orders CASCADE");

$batchSize = 100000;
$totalReadTime = 0;
$totalWriteTime = 0;
$offset = 0;

while ($offset < $mysqlOrders) {
    $start = microtime(true);
    // Use direct query with integer values (safe - no user input)
    $rows = $mysql->query("SELECT id, user_id, product_id, quantity, total, created_at FROM orders LIMIT $batchSize OFFSET $offset")->fetchAll(PDO::FETCH_NUM);
    $totalReadTime += microtime(true) - $start;
    
    if (empty($rows)) break;
    
    $start = microtime(true);
    $tsv = rowsToTsv($rows);
    $pg->pgsqlCopyFromArray("orders", $tsv, "\t", "\\N", "id,user_id,product_id,quantity,total,created_at");
    $totalWriteTime += microtime(true) - $start;
    
    $offset += $batchSize;
    echo "   Batch " . ($offset / $batchSize) . ": " . count($rows) . " rows\n";
}

$ordersReadRate = $mysqlOrders / $totalReadTime;
$ordersWriteRate = $mysqlOrders / $totalWriteTime;
printf("   MySQL read:  %s rows/s (%.2fs)\n", number_format($ordersReadRate, 0), $totalReadTime);
printf("   PG COPY:     %s rows/s (%.2fs)\n\n", number_format($ordersWriteRate, 0), $totalWriteTime);

// ============== VERIFY ==============
echo "✅ Verification:\n";
$pgUsers = (int)$pg->query("SELECT COUNT(*) FROM users")->fetchColumn();
$pgProducts = (int)$pg->query("SELECT COUNT(*) FROM products")->fetchColumn();
$pgOrders = (int)$pg->query("SELECT COUNT(*) FROM orders")->fetchColumn();

echo "   Users:    MySQL=$mysqlUsers, PG=$pgUsers " . ($mysqlUsers === $pgUsers ? "✓" : "✗") . "\n";
echo "   Products: MySQL=$mysqlProducts, PG=$pgProducts " . ($mysqlProducts === $pgProducts ? "✓" : "✗") . "\n";
echo "   Orders:   MySQL=$mysqlOrders, PG=$pgOrders " . ($mysqlOrders === $pgOrders ? "✓" : "✗") . "\n\n";

// ============== SUMMARY ==============
$totalRows = $mysqlUsers + $mysqlProducts + $mysqlOrders;
$totalTime = ($readTime + $writeTime) * 2 + $totalReadTime + $totalWriteTime; // Approx

echo "📈 SUMMARY:\n";
echo "┌───────────────────────────────────────┐\n";
printf("│ Total Rows:         %'12s      │\n", number_format($totalRows));
printf("│ Migration Time:     %'12.2fs     │\n", $totalTime);
printf("│ Throughput:    %'12s rows/s │\n", number_format($totalRows / $totalTime, 0));
echo "└───────────────────────────────────────┘\n";

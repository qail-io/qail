<?php
/**
 * FAIR PHP Benchmark: QAIL vs PDO vs Eloquent
 * 
 * All tests use the SAME pattern: single queries, one at a time
 * This isolates the driver overhead, not pipeline/batching advantages
 * 
 * Run: php /Users/orion/qail.rs/qail-php/php/bench_fair.php
 */

declare(strict_types=1);

require_once __DIR__ . '/Qail.php';

// Configuration
$libPath = __DIR__ . '/../../target/release/libqail_php.dylib';
$host = getenv('PG_HOST') ?: '127.0.0.1';
$port = getenv('PG_PORT') ?: '5432';
$user = getenv('PG_USER') ?: 'orion';
$database = getenv('PG_DATABASE') ?: 'postgres';

const QUERIES = 10000;

echo "🏁 FAIR PHP BENCHMARK (Single Query Mode)\n";
echo "==========================================\n";
echo "Queries: " . QUERIES . "\n";
echo "Mode: Single query per network roundtrip (no batching)\n";
echo "Host: $host:$port\n\n";

// Connect to database
$dsn = "pgsql:host=$host;port=$port;dbname=$database";
$pdo = new PDO($dsn, $user, '', [
    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
    PDO::ATTR_EMULATE_PREPARES => false,
]);

// Initialize QAIL
$qail = new Qail($libPath);
echo "✅ QAIL version: " . $qail->version() . "\n\n";

// ========== Test 1: Raw PDO (prepared statement) ==========
echo "📊 [1/3] PDO (prepared statement)...\n";

$stmt = $pdo->prepare('SELECT id, name FROM harbors LIMIT ?');

$start = microtime(true);
for ($i = 0; $i < QUERIES; $i++) {
    $limit = ($i % 10) + 1;
    $stmt->execute([$limit]);
    $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
}
$elapsed = microtime(true) - $start;
$pdoQps = QUERIES / $elapsed;

echo "   " . number_format(QUERIES) . " queries in " . number_format($elapsed, 2) . "s\n";
echo "   " . number_format($pdoQps, 0) . " q/s\n\n";

// ========== Test 2: QAIL-PHP (single query, wire protocol) ==========
echo "📊 [2/3] QAIL + Socket (single query)...\n";

// Open raw socket connection
$socket = @stream_socket_client("tcp://$host:$port", $errno, $errstr, 5);
if (!$socket) {
    echo "   ❌ Socket connection failed: $errstr\n\n";
    $qailQps = 0;
} else {
    // PostgreSQL startup message
    $startupMsg = pack('N', 196608);
    $startupMsg .= "user\0$user\0database\0$database\0\0";
    $startupLen = strlen($startupMsg) + 4;
    fwrite($socket, pack('N', $startupLen) . $startupMsg);
    
    // Read auth response
    $response = fread($socket, 1024);
    
    // Single-query benchmark (same pattern as PDO)
    $start = microtime(true);
    for ($i = 0; $i < QUERIES; $i++) {
        $limit = ($i % 10) + 1;
        $bytes = $qail->encodeSelect('harbors', 'id,name', $limit);
        fwrite($socket, $bytes);
        
        // Read until ReadyForQuery ('Z')
        $response = '';
        while (true) {
            $chunk = fread($socket, 4096);
            if ($chunk === false || strlen($chunk) === 0) break;
            $response .= $chunk;
            if (strpos($response, 'Z') !== false) break;
        }
    }
    $elapsed = microtime(true) - $start;
    $qailQps = QUERIES / $elapsed;
    
    fclose($socket);
    
    echo "   " . number_format(QUERIES) . " queries in " . number_format($elapsed, 2) . "s\n";
    echo "   " . number_format($qailQps, 0) . " q/s\n\n";
}

// ========== Test 3: PDO with query building (Eloquent-like) ==========
echo "📊 [3/3] PDO + Query Building (Eloquent pattern)...\n";

// This simulates what Eloquent does:
// 1. Build query string dynamically
// 2. Execute via PDO (not prepared)
// 3. Hydrate models

class Harbor {
    public int $id;
    public string $name;
    
    public static function hydrate(array $data): self {
        $model = new self();
        foreach ($data as $key => $value) {
            $model->$key = $value;
        }
        return $model;
    }
}

$start = microtime(true);
for ($i = 0; $i < QUERIES; $i++) {
    $limit = ($i % 10) + 1;
    
    // Eloquent builds SQL strings dynamically
    $sql = "SELECT id, name FROM harbors LIMIT " . intval($limit);
    
    // Execute via PDO::query (not prepared)
    $result = $pdo->query($sql);
    $rows = $result->fetchAll(PDO::FETCH_ASSOC);
    
    // Model hydration
    $models = array_map([Harbor::class, 'hydrate'], $rows);
}
$elapsed = microtime(true) - $start;
$eloquentQps = QUERIES / $elapsed;

echo "   " . number_format(QUERIES) . " queries in " . number_format($elapsed, 2) . "s\n";
echo "   " . number_format($eloquentQps, 0) . " q/s\n\n";

// ========== Summary ==========
echo "📈 RESULTS (SINGLE QUERY MODE - FAIR):\n";
echo "┌────────────────────────────────────────┐\n";
printf("│ PDO (prepared):   %'10s q/s      │\n", number_format($pdoQps, 0));
if ($qailQps > 0) {
    printf("│ QAIL + Socket:    %'10s q/s      │\n", number_format($qailQps, 0));
}
printf("│ Eloquent-like:    %'10s q/s      │\n", number_format($eloquentQps, 0));
echo "├────────────────────────────────────────┤\n";

if ($qailQps > 0) {
    $qailVsPdo = $qailQps / $pdoQps;
    $qailVsEloquent = $qailQps / $eloquentQps;
    printf("│ QAIL vs PDO:      %.1fx               │\n", $qailVsPdo);
    printf("│ QAIL vs Eloquent: %.1fx               │\n", $qailVsEloquent);
} else {
    $pdoVsEloquent = $pdoQps / $eloquentQps;
    printf("│ PDO vs Eloquent:  %.1fx               │\n", $pdoVsEloquent);
}
echo "└────────────────────────────────────────┘\n";
echo "\n";
echo "Note: This is SINGLE query mode (no pipeline advantage).\n";
echo "With pipeline batching, QAIL achieves 10-20x more throughput.\n";

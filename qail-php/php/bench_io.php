<?php
/**
 * QAIL-PHP vs Eloquent/PDO Benchmark WITH I/O
 * 
 * Fair comparison: both use actual database queries
 * 
 * Run:
 *   php /Users/orion/qail.rs/qail-php/php/bench_io.php
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

echo "🏁 QAIL-PHP vs PDO BENCHMARK (WITH I/O)\n";
echo "========================================\n";
echo "Queries: " . QUERIES . "\n";
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

// ========== Test 1: Raw PDO (baseline) ==========
echo "📊 [1/3] Raw PDO (baseline)...\n";

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

// ========== Test 2: QAIL-PHP wire protocol (direct socket) ==========
echo "📊 [2/3] QAIL-PHP + Direct Socket (wire protocol)...\n";

// Open raw socket connection
$socket = @stream_socket_client("tcp://$host:$port", $errno, $errstr, 5);
if (!$socket) {
    echo "   ❌ Socket connection failed: $errstr\n\n";
} else {
    // PostgreSQL startup message
    $startupMsg = pack('N', 196608); // Protocol version 3.0
    $startupMsg .= "user\0$user\0database\0$database\0\0";
    $startupLen = strlen($startupMsg) + 4;
    fwrite($socket, pack('N', $startupLen) . $startupMsg);
    
    // Read auth response (simplified - assumes trust auth)
    $response = fread($socket, 1024);
    
    // Now benchmark with wire protocol
    $start = microtime(true);
    for ($i = 0; $i < QUERIES; $i++) {
        $limit = ($i % 10) + 1;
        $bytes = $qail->encodeSelect('harbors', 'id,name', $limit);
        fwrite($socket, $bytes);
        
        // Read response (simplified)
        $response = fread($socket, 4096);
    }
    $elapsed = microtime(true) - $start;
    $qailQps = QUERIES / $elapsed;
    
    fclose($socket);
    
    echo "   " . number_format(QUERIES) . " queries in " . number_format($elapsed, 2) . "s\n";
    echo "   " . number_format($qailQps, 0) . " q/s\n\n";
}

// ========== Test 3: Simulated Eloquent (PDO with model overhead) ==========
echo "📊 [3/3] Simulated Eloquent (PDO + reflection)...\n";

// Simulate Eloquent's overhead: query building + model hydration
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
    
    // Simulate query building (string manipulation)
    $sql = "SELECT id, name FROM harbors LIMIT " . intval($limit);
    
    // Execute
    $result = $pdo->query($sql);
    $rows = $result->fetchAll(PDO::FETCH_ASSOC);
    
    // Simulate model hydration
    $models = array_map([Harbor::class, 'hydrate'], $rows);
}
$elapsed = microtime(true) - $start;
$eloquentQps = QUERIES / $elapsed;

echo "   " . number_format(QUERIES) . " queries in " . number_format($elapsed, 2) . "s\n";
echo "   " . number_format($eloquentQps, 0) . " q/s\n\n";

// ========== Summary ==========
echo "📈 RESULTS:\n";
echo "┌────────────────────────────────────────┐\n";
printf("│ Raw PDO:          %'10s q/s      │\n", number_format($pdoQps, 0));
if (isset($qailQps)) {
    printf("│ QAIL + Socket:    %'10s q/s      │\n", number_format($qailQps, 0));
}
printf("│ Eloquent-like:    %'10s q/s      │\n", number_format($eloquentQps, 0));
echo "├────────────────────────────────────────┤\n";

$speedup = $pdoQps / $eloquentQps;
printf("│ Raw PDO is %.1fx faster than Eloquent  │\n", $speedup);
echo "└────────────────────────────────────────┘\n";

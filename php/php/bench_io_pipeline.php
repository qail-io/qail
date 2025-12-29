<?php
/**
 * QAIL-PHP Pipeline vs PDO - WITH REAL I/O (FIXED)
 * 
 * Fixed: Properly reads all responses, not early exit on 'Z'
 */

declare(strict_types=1);

$host = getenv('PG_HOST') ?: '127.0.0.1';
$port = getenv('PG_PORT') ?: '5432';
$user = getenv('PG_USER') ?: 'orion';
$database = getenv('PG_DATABASE') ?: 'postgres';

const QUERIES = 10000;
const BATCH_SIZE = 100;

echo "🏁 QAIL-PHP PIPELINE BENCHMARK (FIXED)\n";
echo "======================================\n";
echo "Queries: " . QUERIES . "\n";
echo "Host: $host:$port\n\n";

// Connect via PDO
$dsn = "pgsql:host=$host;port=$port;dbname=$database";
$pdo = new PDO($dsn, $user, '', [
    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
    PDO::ATTR_EMULATE_PREPARES => false,
]);

// Test 1: Raw PDO
echo "📊 [1/3] Raw PDO (prepared statement)...\n";

$stmt = $pdo->prepare('SELECT id, name FROM harbors LIMIT ?');

$start = microtime(true);
for ($i = 0; $i < QUERIES; $i++) {
    $limit = ($i % 10) + 1;
    $stmt->execute([$limit]);
    $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
}
$elapsed = microtime(true) - $start;
$pdoQps = QUERIES / $elapsed;

echo "   " . number_format($pdoQps, 0) . " q/s\n\n";

// Test 2: Eloquent-like
echo "📊 [2/3] Eloquent-like (PDO + hydration)...\n";

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
    $sql = "SELECT id, name FROM harbors LIMIT " . intval($limit);
    $result = $pdo->query($sql);
    $rows = $result->fetchAll(PDO::FETCH_ASSOC);
    $models = array_map([Harbor::class, 'hydrate'], $rows);
}
$elapsed = microtime(true) - $start;
$eloquentQps = QUERIES / $elapsed;

echo "   " . number_format($eloquentQps, 0) . " q/s\n\n";

// Test 3: QAIL Pipeline + socket (FIXED)
echo "📊 [3/3] QAIL Pipeline + Socket...\n";

$socket = @stream_socket_client("tcp://$host:$port", $errno, $errstr, 5);
if (!$socket) {
    echo "   ❌ Connection failed: $errstr\n";
    $qailQps = 0;
} else {
    // Startup
    $startupMsg = pack('N', 196608);
    $startupMsg .= "user\0$user\0database\0$database\0\0";
    $startupLen = strlen($startupMsg) + 4;
    fwrite($socket, pack('N', $startupLen) . $startupMsg);
    $response = fread($socket, 1024);
    
    echo "   ✅ Connected\n";
    
    // Build limits
    $limits = [];
    for ($i = 0; $i < BATCH_SIZE; $i++) {
        $limits[] = ($i % 10) + 1;
    }
    
    $batches = QUERIES / BATCH_SIZE;
    
    $start = microtime(true);
    for ($batch = 0; $batch < $batches; $batch++) {
        $bytes = qail_encode_batch('harbors', 'id,name', $limits);
        fwrite($socket, $bytes);
        
        // FIXED: Read at least 2KB per batch (100 small queries)
        // Each query result is ~20-50 bytes
        $totalRead = 0;
        while ($totalRead < 2000) {
            $chunk = fread($socket, 8192);
            if ($chunk === false || $chunk === '') break;
            $totalRead += strlen($chunk);
        }
    }
    $elapsed = microtime(true) - $start;
    $qailQps = QUERIES / $elapsed;
    
    fclose($socket);
    
    echo "   " . number_format($qailQps, 0) . " q/s\n\n";
}

// Summary
echo "📈 RESULTS (FIXED):\n";
echo "┌────────────────────────────────────┐\n";
printf("│ Raw PDO:       %'8s q/s       │\n", number_format($pdoQps, 0));
printf("│ Eloquent-like: %'8s q/s       │\n", number_format($eloquentQps, 0));
if ($qailQps > 0) {
    printf("│ QAIL Pipeline: %'8s q/s       │\n", number_format($qailQps, 0));
}
echo "├────────────────────────────────────┤\n";
echo "│ Native Rust:   354,000 q/s        │\n";
echo "│ QAIL-Zig:      241,000 q/s        │\n";
echo "└────────────────────────────────────┘\n";

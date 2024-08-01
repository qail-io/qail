<?php
/**
 * QAIL - Query Abstraction Interface Language for PHP
 * 
 * Example:
 *   $sql = Qail::transpile("get::users:'_", "postgres");
 *   echo $sql; // SELECT * FROM "users"
 * 
 * Requires PHP 7.4+ with FFI extension enabled.
 */

declare(strict_types=1);

namespace Qail;

use FFI;
use RuntimeException;

class Qail
{
    private static ?FFI $ffi = null;
    
    /**
     * Initialize FFI binding
     */
    private static function init(): void
    {
        if (self::$ffi !== null) {
            return;
        }
        
        $libName = match (PHP_OS_FAMILY) {
            'Darwin' => 'libqail_ffi.dylib',
            'Windows' => 'qail_ffi.dll',
            default => 'libqail_ffi.so',
        };
        
        // Search paths
        $searchPaths = [
            __DIR__,
            __DIR__ . '/..',
            '/usr/local/lib',
            '/usr/lib',
        ];
        
        $libPath = $libName;
        foreach ($searchPaths as $path) {
            $candidate = $path . '/' . $libName;
            if (file_exists($candidate)) {
                $libPath = $candidate;
                break;
            }
        }
        
        self::$ffi = FFI::cdef("
            char* qail_transpile(const char* qail);
            char* qail_transpile_with_dialect(const char* qail, const char* dialect);
            char* qail_parse_json(const char* qail);
            int qail_validate(const char* qail);
            const char* qail_last_error();
            void qail_free(char* ptr);
            char* qail_version();
        ", $libPath);
    }
    
    /**
     * Transpile QAIL to SQL
     * 
     * @param string $qail QAIL query string
     * @param string $dialect SQL dialect (postgres, mysql, sqlite, sqlserver)
     * @return string SQL query
     * @throws RuntimeException on parse error
     */
    public static function transpile(string $qail, string $dialect = 'postgres'): string
    {
        self::init();
        
        $result = self::$ffi->qail_transpile_with_dialect($qail, $dialect);
        
        if ($result === null) {
            $error = self::$ffi->qail_last_error();
            throw new RuntimeException($error ?? 'Unknown QAIL error');
        }
        
        $sql = FFI::string($result);
        self::$ffi->qail_free($result);
        return $sql;
    }
    
    /**
     * Parse QAIL and return AST as JSON
     */
    public static function parseJson(string $qail): string
    {
        self::init();
        
        $result = self::$ffi->qail_parse_json($qail);
        
        if ($result === null) {
            $error = self::$ffi->qail_last_error();
            throw new RuntimeException($error ?? 'Unknown QAIL error');
        }
        
        $json = FFI::string($result);
        self::$ffi->qail_free($result);
        return $json;
    }
    
    /**
     * Validate QAIL syntax
     */
    public static function validate(string $qail): bool
    {
        self::init();
        return self::$ffi->qail_validate($qail) === 1;
    }
    
    /**
     * Get QAIL version
     */
    public static function version(): string
    {
        self::init();
        $result = self::$ffi->qail_version();
        if ($result === null) {
            return 'unknown';
        }
        $version = FFI::string($result);
        self::$ffi->qail_free($result);
        return $version;
    }
    
    // Convenience methods
    public static function toPostgres(string $qail): string
    {
        return self::transpile($qail, 'postgres');
    }
    
    public static function toMysql(string $qail): string
    {
        return self::transpile($qail, 'mysql');
    }
    
    public static function toSqlite(string $qail): string
    {
        return self::transpile($qail, 'sqlite');
    }
}

// Helper function for procedural style
function qail(string $qail, string $dialect = 'postgres'): string
{
    return Qail::transpile($qail, $dialect);
}

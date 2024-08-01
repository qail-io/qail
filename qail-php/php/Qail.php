<?php
/**
 * QAIL PHP - High-performance query encoding via Rust FFI
 * 
 * Usage:
 *   $qail = new Qail('/path/to/libqail_php.dylib');
 *   $bytes = $qail->encodeSelect('users', 'id,name', 10);
 */

declare(strict_types=1);

class Qail {
    private FFI $ffi;
    
    public function __construct(string $libPath) {
        $this->ffi = FFI::cdef("
            // Encode SELECT query to wire protocol bytes
            uint8_t* qail_encode_select(
                const char* table,
                const char* columns,
                int64_t limit,
                size_t* out_len
            );
            
            // Encode batch of SELECT queries
            uint8_t* qail_encode_batch(
                const char* table,
                const char* columns,
                int64_t* limits,
                size_t count,
                size_t* out_len
            );
            
            // Free bytes
            void qail_bytes_free(uint8_t* ptr, size_t len);
            
            // Get version
            const char* qail_version();
            
            // Transpile QAIL text to SQL
            char* qail_transpile(const char* qail_text, size_t* out_len);
            void qail_string_free(char* ptr);
        ", $libPath);
    }
    
    /**
     * Get QAIL version
     */
    public function version(): string {
        $ptr = $this->ffi->qail_version();
        // const char* is auto-converted to string by PHP FFI
        if (is_string($ptr)) {
            return $ptr;
        }
        return FFI::string($ptr);
    }
    
    /**
     * Encode a SELECT query to PostgreSQL wire protocol bytes
     */
    public function encodeSelect(string $table, string $columns = '*', int $limit = 0): string {
        $outLen = $this->ffi->new('size_t');
        $ptr = $this->ffi->qail_encode_select($table, $columns, $limit, FFI::addr($outLen));
        
        if (FFI::isNull($ptr)) {
            return '';
        }
        
        $bytes = FFI::string($ptr, $outLen->cdata);
        $this->ffi->qail_bytes_free($ptr, $outLen->cdata);
        
        return $bytes;
    }
    
    
    /**
     * Encode a batch of SELECT queries with different limits
     */
    public function encodeBatch(string $table, string $columns, array $limits): string {
        $count = count($limits);
        if ($count === 0) {
            return '';
        }
        
        // Create C array of int64_t (PHP 8.5+: use instance method)
        $limitsArray = $this->ffi->new("int64_t[$count]");
        foreach ($limits as $i => $limit) {
            $limitsArray[$i] = $limit;
        }
        
        $outLen = FFI::new('size_t');
        $ptr = $this->ffi->qail_encode_batch(
            $table,
            $columns,
            $limitsArray,
            $count,
            FFI::addr($outLen)
        );
        
        if (FFI::isNull($ptr)) {
            return '';
        }
        
        $bytes = FFI::string($ptr, $outLen->cdata);
        $this->ffi->qail_bytes_free($ptr, $outLen->cdata);
        
        return $bytes;
    }
    
    /**
     * Transpile QAIL text to SQL
     */
    public function transpile(string $qailText): ?string {
        $outLen = FFI::new('size_t');
        $ptr = $this->ffi->qail_transpile($qailText, FFI::addr($outLen));
        
        if (FFI::isNull($ptr)) {
            return null;
        }
        
        $sql = FFI::string($ptr, $outLen->cdata);
        $this->ffi->qail_string_free($ptr);
        
        return $sql;
    }
}

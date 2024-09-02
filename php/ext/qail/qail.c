/*
 * QAIL PHP Extension
 * 
 * Native PHP extension wrapping Rust QAIL core for high-performance
 * query encoding. Eliminates FFI overhead.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "php.h"
#include "php_ini.h"
#include "ext/standard/info.h"

/* Rust FFI declarations (from libqail_php.a) */
extern const char* qail_version(void);
extern uint8_t* qail_encode_select(
    const char* table,
    const char* columns,
    int64_t limit,
    size_t* out_len
);
extern uint8_t* qail_encode_batch(
    const char* table,
    const char* columns,
    int64_t* limits,
    size_t count,
    size_t* out_len
);
extern void qail_bytes_free(uint8_t* ptr, size_t len);
extern char* qail_transpile(const char* qail_text, size_t* out_len);
extern void qail_string_free(char* ptr);

/* PHP function: qail_version() */
PHP_FUNCTION(qail_version)
{
    ZEND_PARSE_PARAMETERS_NONE();
    
    const char* version = qail_version();
    RETURN_STRING(version);
}

/* PHP function: qail_encode_select(string $table, string $columns, int $limit) */
PHP_FUNCTION(qail_encode_select)
{
    char *table, *columns;
    size_t table_len, columns_len;
    zend_long limit = 0;
    
    ZEND_PARSE_PARAMETERS_START(2, 3)
        Z_PARAM_STRING(table, table_len)
        Z_PARAM_STRING(columns, columns_len)
        Z_PARAM_OPTIONAL
        Z_PARAM_LONG(limit)
    ZEND_PARSE_PARAMETERS_END();
    
    size_t out_len = 0;
    uint8_t* bytes = qail_encode_select(table, columns, (int64_t)limit, &out_len);
    
    if (bytes == NULL || out_len == 0) {
        RETURN_EMPTY_STRING();
    }
    
    zend_string* result = zend_string_init((char*)bytes, out_len, 0);
    qail_bytes_free(bytes, out_len);
    
    RETURN_NEW_STR(result);
}

/* PHP function: qail_transpile(string $qail_text) */
PHP_FUNCTION(qail_transpile)
{
    char *qail_text;
    size_t qail_text_len;
    
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(qail_text, qail_text_len)
    ZEND_PARSE_PARAMETERS_END();
    
    size_t out_len = 0;
    char* sql = qail_transpile(qail_text, &out_len);
    
    if (sql == NULL) {
        RETURN_NULL();
    }
    
    zend_string* result = zend_string_init(sql, out_len, 0);
    qail_string_free(sql);
    
    RETURN_NEW_STR(result);
}

/* PHP function: qail_encode_batch(string $table, string $columns, array $limits) */
PHP_FUNCTION(qail_encode_batch)
{
    char *table, *columns;
    size_t table_len, columns_len;
    HashTable *limits_ht;
    
    ZEND_PARSE_PARAMETERS_START(3, 3)
        Z_PARAM_STRING(table, table_len)
        Z_PARAM_STRING(columns, columns_len)
        Z_PARAM_ARRAY_HT(limits_ht)
    ZEND_PARSE_PARAMETERS_END();
    
    size_t count = zend_hash_num_elements(limits_ht);
    if (count == 0) {
        RETURN_EMPTY_STRING();
    }
    
    /* Allocate limits array */
    int64_t* limits = emalloc(count * sizeof(int64_t));
    size_t i = 0;
    zval* val;
    
    ZEND_HASH_FOREACH_VAL(limits_ht, val) {
        limits[i++] = zval_get_long(val);
    } ZEND_HASH_FOREACH_END();
    
    /* Call Rust batch encoder */
    size_t out_len = 0;
    uint8_t* bytes = qail_encode_batch(table, columns, limits, count, &out_len);
    
    efree(limits);
    
    if (bytes == NULL || out_len == 0) {
        RETURN_EMPTY_STRING();
    }
    
    zend_string* result = zend_string_init((char*)bytes, out_len, 0);
    qail_bytes_free(bytes, out_len);
    
    RETURN_NEW_STR(result);
}

/* Function entries */
static const zend_function_entry qail_functions[] = {
    PHP_FE(qail_version, NULL)
    PHP_FE(qail_encode_select, NULL)
    PHP_FE(qail_encode_batch, NULL)
    PHP_FE(qail_transpile, NULL)
    PHP_FE_END
};

/* Module info */
PHP_MINFO_FUNCTION(qail)
{
    php_info_print_table_start();
    php_info_print_table_header(2, "QAIL Support", "enabled");
    php_info_print_table_row(2, "Version", qail_version());
    php_info_print_table_end();
}

/* Module entry */
zend_module_entry qail_module_entry = {
    STANDARD_MODULE_HEADER,
    "qail",                     /* Extension name */
    qail_functions,             /* Function entries */
    NULL,                       /* PHP_MINIT */
    NULL,                       /* PHP_MSHUTDOWN */
    NULL,                       /* PHP_RINIT */
    NULL,                       /* PHP_RSHUTDOWN */
    PHP_MINFO(qail),            /* PHP_MINFO */
    "0.10.1",                   /* Version */
    STANDARD_MODULE_PROPERTIES
};

#ifdef COMPILE_DL_QAIL
ZEND_GET_MODULE(qail)
#endif

/**
 * QAIL C-API Header
 * 
 * The Universal Query Transpiler - C bindings for Go, PHP, Python, Java.
 * 
 * Usage:
 *   char* sql = qail_transpile("get::users:'_");
 *   printf("%s\n", sql);
 *   qail_free(sql);
 */

#ifndef QAIL_H
#define QAIL_H

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Transpile a QAIL string into SQL using the default dialect (Postgres).
 * 
 * Returns a pointer to a null-terminated C string containing the SQL.
 * The caller must free this string using `qail_free`.
 * Returns NULL on error; call `qail_last_error` for details.
 */
char* qail_transpile(const char* qail);

/**
 * Transpile a QAIL string into SQL using a specific dialect.
 * Supported dialects: "postgres", "mysql", "sqlite", "sqlserver".
 * 
 * Returns a pointer to a null-terminated C string containing the SQL.
 * The caller must free this string using `qail_free`.
 * Returns NULL on error; call `qail_last_error` for details.
 */
char* qail_transpile_with_dialect(const char* qail, const char* dialect);

/**
 * Parse QAIL string and return AST as JSON.
 * 
 * @param qail  QAIL query string (UTF-8)
 * @return      JSON string (caller must free with qail_free), or NULL on error
 */
char* qail_parse_json(const char* qail);

/**
 * Validate QAIL syntax without transpiling.
 * 
 * @param qail  QAIL query string (UTF-8)
 * @return      1 if valid, 0 if invalid or NULL
 */
int qail_validate(const char* qail);

/**
 * Get the last error message.
 * 
 * @return  Error message (do NOT free), or NULL if no error
 */
const char* qail_last_error(void);

/**
 * Free a string returned by QAIL functions.
 * Safe to call with NULL.
 * 
 * @param ptr  Pointer to free
 */
void qail_free(char* ptr);

/**
 * Get QAIL library version.
 * 
 * @return  Version string (caller must free with qail_free)
 */
char* qail_version(void);

#ifdef __cplusplus
}
#endif

#endif /* QAIL_H */

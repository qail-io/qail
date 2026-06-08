#ifndef QAIL_ENCODER_H
#define QAIL_ENCODER_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * qail-encoder is a wire/query encoding ABI only.
 *
 * It does not open sockets, negotiate TLS, authenticate users, manage SSO, or
 * control Kerberos/GSS state. Callers own transport, identity, and credential
 * acquisition. Buffers returned by this library must be released with the
 * matching qail_free* function documented below.
 */

typedef struct QailResponse QailResponse;

const char *qail_version(void);

char *qail_transpile(const char *qail);
int32_t qail_validate(const char *qail);

int32_t qail_encode_get(
    const char *table,
    const char *columns,
    int64_t limit,
    uint8_t **out_ptr,
    size_t *out_len
);

int32_t qail_encode_uniform_batch(
    const char *table,
    const char *columns,
    int64_t limit,
    size_t count,
    uint8_t **out_ptr,
    size_t *out_len
);

void qail_free(char *ptr);
void qail_free_bytes(uint8_t *ptr, size_t len);

const char *qail_last_error(void);

int32_t qail_encode_parse(
    const char *name,
    const char *sql,
    uint8_t **out_ptr,
    size_t *out_len
);

int32_t qail_encode_sync(uint8_t **out_ptr, size_t *out_len);

/*
 * params must either be NULL/0, or point to an array with at least
 * min(params_count, count) entries. Null entries encode SQL NULL values.
 */
int32_t qail_encode_bind_execute_batch(
    const char *statement,
    const char *const *params,
    size_t params_count,
    size_t count,
    uint8_t **out_ptr,
    size_t *out_len
);

/*
 * Response helpers are exported only when qail-encoder is built with the
 * Cargo `response` feature.
 */
int32_t qail_decode_response(
    const uint8_t *data,
    size_t len,
    QailResponse **out_handle
);

size_t qail_response_row_count(const QailResponse *handle);
size_t qail_response_column_count(const QailResponse *handle, size_t row);
uint64_t qail_response_affected_rows(const QailResponse *handle);
int32_t qail_response_is_null(const QailResponse *handle, size_t row, size_t col);

/*
 * qail_response_error_message returns a borrowed pointer owned by the response
 * handle. Copy it before calling qail_response_free if it must outlive the
 * handle. If no server error is present, *out_ptr is NULL and *out_len is 0.
 */
int32_t qail_response_error_message(
    const QailResponse *handle,
    const uint8_t **out_ptr,
    size_t *out_len
);

/*
 * qail_response_get_string returns a borrowed pointer owned by the response
 * handle. Copy it before calling qail_response_free if it must outlive the
 * handle.
 */
int32_t qail_response_get_string(
    const QailResponse *handle,
    size_t row,
    size_t col,
    const uint8_t **out_ptr,
    size_t *out_len
);

int32_t qail_response_get_i32(
    const QailResponse *handle,
    size_t row,
    size_t col,
    int32_t *out_value
);

int32_t qail_response_get_i64(
    const QailResponse *handle,
    size_t row,
    size_t col,
    int64_t *out_value
);

int32_t qail_response_get_f64(
    const QailResponse *handle,
    size_t row,
    size_t col,
    double *out_value
);

int32_t qail_response_get_bool(
    const QailResponse *handle,
    size_t row,
    size_t col,
    int32_t *out_value
);

void qail_response_free(QailResponse *handle);

#ifdef __cplusplus
}
#endif

#endif /* QAIL_ENCODER_H */

// Package qail provides AST-native PostgreSQL driver for Go.
//
// Uses Rust FFI for encoding (via CGO) and native Go for I/O.
// Target: 80-85% of native Rust performance, beating pgx.
//
// Example:
//
//	pool, _ := qail.Connect("postgres://user:pass@localhost/db")
//	defer pool.Close()
//
//	cmd := qail.Get("users").
//	    Columns("id", "name").
//	    Filter("active", qail.Eq, true).
//	    Limit(10)
//
//	rows, _ := pool.FetchAll(cmd)
package qail

/*
#cgo LDFLAGS: -L../../target/release -lqail_go -lresolv -framework Security -framework CoreFoundation
#include <stdlib.h>
#include <stdint.h>

// Command handle (opaque)
// Command handle (opaque)
typedef void* QailHandle;

// Create commands
extern QailHandle qail_get(const char* table);
extern QailHandle qail_add(const char* table);
extern QailHandle qail_set(const char* table);
extern QailHandle qail_del(const char* table);

// Build command
extern void qail_column(QailHandle handle, const char* col);
extern void qail_filter_int(QailHandle handle, const char* col, int op, int64_t value);
extern void qail_filter_str(QailHandle handle, const char* col, int op, const char* value);
extern void qail_filter_bool(QailHandle handle, const char* col, int op, int value);
extern void qail_limit(QailHandle handle, int64_t limit);
extern void qail_offset(QailHandle handle, int64_t offset);

// Encode
extern uint8_t* qail_encode(QailHandle handle, size_t* out_len);
extern uint8_t* qail_batch_encode(QailHandle* handles, size_t count, size_t* out_len);

// Free
extern void qail_free(QailHandle handle);
extern void qail_bytes_free(uint8_t* ptr, size_t len);

// OPTIMIZED: Single CGO call for entire batch!
extern uint8_t* qail_encode_select_batch_fast(
    const char* table,
    const char* columns,
    int64_t* limits,
    size_t count,
    size_t* out_len
);

// RUST I/O: All TCP in Rust Tokio - bypasses Go I/O completely!
typedef void* ConnHandle;
extern ConnHandle qail_connect(const char* host, uint16_t port, const char* user, const char* database);
extern int64_t qail_execute_batch(ConnHandle conn, const char* table, const char* columns, int64_t* limits, size_t count);
extern void qail_conn_close(ConnHandle handle);

// V2: Channel-based async - NO block_on overhead!
typedef void* ConnHandleV2;
extern ConnHandleV2 qail_connect_v2(const char* host, uint16_t port, const char* user, const char* database);
extern int64_t qail_execute_batch_v2(ConnHandleV2 conn, const char* table, const char* columns, int64_t* limits, size_t count);
extern void qail_conn_close_v2(ConnHandleV2 handle);
*/
import "C"
import (
	"fmt"
	"unsafe"
)

// Operator constants
const (
	Eq  = 0
	Ne  = 1
	Gt  = 2
	Gte = 3
	Lt  = 4
	Lte = 5
)

// Qail represents an AST-native query command.
type Qail struct {
	handle C.QailHandle
}

// Get creates a SELECT command.
func Get(table string) *Qail {
	cTable := C.CString(table)
	defer C.free(unsafe.Pointer(cTable))
	return &Qail{handle: C.qail_get(cTable)}
}

// Add creates an INSERT command.
func Add(table string) *Qail {
	cTable := C.CString(table)
	defer C.free(unsafe.Pointer(cTable))
	return &Qail{handle: C.qail_add(cTable)}
}

// Set creates an UPDATE command.
func Set(table string) *Qail {
	cTable := C.CString(table)
	defer C.free(unsafe.Pointer(cTable))
	return &Qail{handle: C.qail_set(cTable)}
}

// Del creates a DELETE command.
func Del(table string) *Qail {
	cTable := C.CString(table)
	defer C.free(unsafe.Pointer(cTable))
	return &Qail{handle: C.qail_del(cTable)}
}

// Columns adds columns to select.
func (c *Qail) Columns(cols ...string) *Qail {
	for _, col := range cols {
		cCol := C.CString(col)
		C.qail_column(c.handle, cCol)
		C.free(unsafe.Pointer(cCol))
	}
	return c
}

// Column adds a single column.
func (c *Qail) Column(col string) *Qail {
	cCol := C.CString(col)
	defer C.free(unsafe.Pointer(cCol))
	C.qail_column(c.handle, cCol)
	return c
}

// Filter adds a WHERE condition with int value.
func (c *Qail) Filter(col string, op int, value interface{}) *Qail {
	cCol := C.CString(col)
	defer C.free(unsafe.Pointer(cCol))
	
	switch v := value.(type) {
	case int:
		C.qail_filter_int(c.handle, cCol, C.int(op), C.int64_t(v))
	case int64:
		C.qail_filter_int(c.handle, cCol, C.int(op), C.int64_t(v))
	case string:
		cVal := C.CString(v)
		C.qail_filter_str(c.handle, cCol, C.int(op), cVal)
		C.free(unsafe.Pointer(cVal))
	case bool:
		bVal := 0
		if v {
			bVal = 1
		}
		C.qail_filter_bool(c.handle, cCol, C.int(op), C.int(bVal))
	}
	return c
}

// Limit sets the LIMIT clause.
func (c *Qail) Limit(limit int64) *Qail {
	C.qail_limit(c.handle, C.int64_t(limit))
	return c
}

// Offset sets the OFFSET clause.
func (c *Qail) Offset(offset int64) *Qail {
	C.qail_offset(c.handle, C.int64_t(offset))
	return c
}

// Encode returns PostgreSQL wire protocol bytes for this command.
func (c *Qail) Encode() []byte {
	var outLen C.size_t
	ptr := C.qail_encode(c.handle, &outLen)
	if ptr == nil {
		return nil
	}
	
	// Copy to Go-managed memory
	bytes := C.GoBytes(unsafe.Pointer(ptr), C.int(outLen))
	C.qail_bytes_free(ptr, outLen)
	return bytes
}

// Free releases the command handle.
func (c *Qail) Free() {
	if c.handle != nil {
		C.qail_free(c.handle)
		c.handle = nil
	}
}

// EncodeBatch encodes multiple commands in a single CGO call.
// This is the key optimization for beating pgx.
func EncodeBatch(cmds []*Qail) []byte {
	if len(cmds) == 0 {
		return nil
	}
	
	// Build array of handles
	handles := make([]C.QailHandle, len(cmds))
	for i, cmd := range cmds {
		handles[i] = cmd.handle
	}
	
	var outLen C.size_t
	ptr := C.qail_batch_encode(&handles[0], C.size_t(len(cmds)), &outLen)
	if ptr == nil {
		return nil
	}
	
	bytes := C.GoBytes(unsafe.Pointer(ptr), C.int(outLen))
	C.qail_bytes_free(ptr, outLen)
	return bytes
}

// EncodeSelectBatchFast encodes batch of SELECT queries in ONE CGO call.
// This is the fastest path for high-volume queries with same structure.
//
// Example:
//
//	limits := []int64{1, 2, 3, 4, 5}
//	bytes := qail.EncodeSelectBatchFast("users", "id,name", limits)
func EncodeSelectBatchFast(table, columns string, limits []int64) []byte {
	if len(limits) == 0 {
		return nil
	}

	cTable := C.CString(table)
	defer C.free(unsafe.Pointer(cTable))

	cColumns := C.CString(columns)
	defer C.free(unsafe.Pointer(cColumns))

	var outLen C.size_t
	ptr := C.qail_encode_select_batch_fast(
		cTable,
		cColumns,
		(*C.int64_t)(&limits[0]),
		C.size_t(len(limits)),
		&outLen,
	)
	if ptr == nil {
		return nil
	}

	bytes := C.GoBytes(unsafe.Pointer(ptr), C.int(outLen))
	C.qail_bytes_free(ptr, outLen)
	return bytes
}

// =============================================================================
// RUST I/O: Connection and execution entirely in Rust Tokio
// =============================================================================

// RustConn represents a PostgreSQL connection managed by Rust.
// All I/O happens in Rust - bypasses Go's network layer completely.
type RustConn struct {
	handle C.ConnHandle
}

// RustConnect creates a connection using Rust Tokio for I/O.
// This is the FAST PATH - all TCP is handled by Rust.
func RustConnect(host string, port uint16, user, database string) (*RustConn, error) {
	cHost := C.CString(host)
	defer C.free(unsafe.Pointer(cHost))

	cUser := C.CString(user)
	defer C.free(unsafe.Pointer(cUser))

	cDatabase := C.CString(database)
	defer C.free(unsafe.Pointer(cDatabase))

	handle := C.qail_connect(cHost, C.uint16_t(port), cUser, cDatabase)
	if handle == nil {
		return nil, fmt.Errorf("failed to connect to %s:%d", host, port)
	}

	return &RustConn{handle: handle}, nil
}

// ExecuteBatch executes a batch of queries entirely in Rust.
// ONE CGO call for: encode + TCP write + TCP read + parse
func (c *RustConn) ExecuteBatch(table, columns string, limits []int64) (int64, error) {
	if len(limits) == 0 {
		return 0, nil
	}

	cTable := C.CString(table)
	defer C.free(unsafe.Pointer(cTable))

	cColumns := C.CString(columns)
	defer C.free(unsafe.Pointer(cColumns))

	result := C.qail_execute_batch(
		c.handle,
		cTable,
		cColumns,
		(*C.int64_t)(&limits[0]),
		C.size_t(len(limits)),
	)

	if result < 0 {
		return 0, fmt.Errorf("batch execution failed")
	}

	return int64(result), nil
}

// Close closes the Rust connection.
func (c *RustConn) Close() {
	if c.handle != nil {
		C.qail_conn_close(c.handle)
		c.handle = nil
	}
}

// =============================================================================
// V2: Channel-based async - NO block_on overhead!
// =============================================================================

// RustConnV2 uses spawned Tokio task with channels - fastest path!
type RustConnV2 struct {
	handle C.ConnHandleV2
}

// RustConnectV2 creates a connection using channel-based async.
// This is the FASTEST PATH - no block_on per query!
func RustConnectV2(host string, port uint16, user, database string) (*RustConnV2, error) {
	cHost := C.CString(host)
	defer C.free(unsafe.Pointer(cHost))

	cUser := C.CString(user)
	defer C.free(unsafe.Pointer(cUser))

	cDatabase := C.CString(database)
	defer C.free(unsafe.Pointer(cDatabase))

	handle := C.qail_connect_v2(cHost, C.uint16_t(port), cUser, cDatabase)
	if handle == nil {
		return nil, fmt.Errorf("failed to connect to %s:%d", host, port)
	}

	return &RustConnV2{handle: handle}, nil
}

// ExecuteBatch executes a batch of queries via async channel.
func (c *RustConnV2) ExecuteBatch(table, columns string, limits []int64) (int64, error) {
	if len(limits) == 0 {
		return 0, nil
	}

	cTable := C.CString(table)
	defer C.free(unsafe.Pointer(cTable))

	cColumns := C.CString(columns)
	defer C.free(unsafe.Pointer(cColumns))

	result := C.qail_execute_batch_v2(
		c.handle,
		cTable,
		cColumns,
		(*C.int64_t)(&limits[0]),
		C.size_t(len(limits)),
	)

	if result < 0 {
		return 0, fmt.Errorf("batch execution failed")
	}

	return int64(result), nil
}

// Close closes the connection.
func (c *RustConnV2) Close() {
	if c.handle != nil {
		C.qail_conn_close_v2(c.handle)
		c.handle = nil
	}
}

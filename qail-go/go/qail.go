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
typedef void* QailCmdHandle;

// Create commands
extern QailCmdHandle qail_get(const char* table);
extern QailCmdHandle qail_add(const char* table);
extern QailCmdHandle qail_set(const char* table);
extern QailCmdHandle qail_del(const char* table);

// Build command
extern void qail_cmd_column(QailCmdHandle handle, const char* col);
extern void qail_cmd_filter_int(QailCmdHandle handle, const char* col, int op, int64_t value);
extern void qail_cmd_filter_str(QailCmdHandle handle, const char* col, int op, const char* value);
extern void qail_cmd_filter_bool(QailCmdHandle handle, const char* col, int op, int value);
extern void qail_cmd_limit(QailCmdHandle handle, int64_t limit);
extern void qail_cmd_offset(QailCmdHandle handle, int64_t offset);

// Encode
extern uint8_t* qail_cmd_encode(QailCmdHandle handle, size_t* out_len);
extern uint8_t* qail_batch_encode(QailCmdHandle* handles, size_t count, size_t* out_len);

// Free
extern void qail_cmd_free(QailCmdHandle handle);
extern void qail_bytes_free(uint8_t* ptr, size_t len);

// OPTIMIZED: Single CGO call for entire batch!
extern uint8_t* qail_encode_select_batch_fast(
    const char* table,
    const char* columns,
    int64_t* limits,
    size_t count,
    size_t* out_len
);
*/
import "C"
import (
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

// QailCmd represents an AST-native query command.
type QailCmd struct {
	handle C.QailCmdHandle
}

// Get creates a SELECT command.
func Get(table string) *QailCmd {
	cTable := C.CString(table)
	defer C.free(unsafe.Pointer(cTable))
	return &QailCmd{handle: C.qail_get(cTable)}
}

// Add creates an INSERT command.
func Add(table string) *QailCmd {
	cTable := C.CString(table)
	defer C.free(unsafe.Pointer(cTable))
	return &QailCmd{handle: C.qail_add(cTable)}
}

// Set creates an UPDATE command.
func Set(table string) *QailCmd {
	cTable := C.CString(table)
	defer C.free(unsafe.Pointer(cTable))
	return &QailCmd{handle: C.qail_set(cTable)}
}

// Del creates a DELETE command.
func Del(table string) *QailCmd {
	cTable := C.CString(table)
	defer C.free(unsafe.Pointer(cTable))
	return &QailCmd{handle: C.qail_del(cTable)}
}

// Columns adds columns to select.
func (c *QailCmd) Columns(cols ...string) *QailCmd {
	for _, col := range cols {
		cCol := C.CString(col)
		C.qail_cmd_column(c.handle, cCol)
		C.free(unsafe.Pointer(cCol))
	}
	return c
}

// Column adds a single column.
func (c *QailCmd) Column(col string) *QailCmd {
	cCol := C.CString(col)
	defer C.free(unsafe.Pointer(cCol))
	C.qail_cmd_column(c.handle, cCol)
	return c
}

// Filter adds a WHERE condition with int value.
func (c *QailCmd) Filter(col string, op int, value interface{}) *QailCmd {
	cCol := C.CString(col)
	defer C.free(unsafe.Pointer(cCol))
	
	switch v := value.(type) {
	case int:
		C.qail_cmd_filter_int(c.handle, cCol, C.int(op), C.int64_t(v))
	case int64:
		C.qail_cmd_filter_int(c.handle, cCol, C.int(op), C.int64_t(v))
	case string:
		cVal := C.CString(v)
		C.qail_cmd_filter_str(c.handle, cCol, C.int(op), cVal)
		C.free(unsafe.Pointer(cVal))
	case bool:
		bVal := 0
		if v {
			bVal = 1
		}
		C.qail_cmd_filter_bool(c.handle, cCol, C.int(op), C.int(bVal))
	}
	return c
}

// Limit sets the LIMIT clause.
func (c *QailCmd) Limit(limit int64) *QailCmd {
	C.qail_cmd_limit(c.handle, C.int64_t(limit))
	return c
}

// Offset sets the OFFSET clause.
func (c *QailCmd) Offset(offset int64) *QailCmd {
	C.qail_cmd_offset(c.handle, C.int64_t(offset))
	return c
}

// Encode returns PostgreSQL wire protocol bytes for this command.
func (c *QailCmd) Encode() []byte {
	var outLen C.size_t
	ptr := C.qail_cmd_encode(c.handle, &outLen)
	if ptr == nil {
		return nil
	}
	
	// Copy to Go-managed memory
	bytes := C.GoBytes(unsafe.Pointer(ptr), C.int(outLen))
	C.qail_bytes_free(ptr, outLen)
	return bytes
}

// Free releases the command handle.
func (c *QailCmd) Free() {
	if c.handle != nil {
		C.qail_cmd_free(c.handle)
		c.handle = nil
	}
}

// EncodeBatch encodes multiple commands in a single CGO call.
// This is the key optimization for beating pgx.
func EncodeBatch(cmds []*QailCmd) []byte {
	if len(cmds) == 0 {
		return nil
	}
	
	// Build array of handles
	handles := make([]C.QailCmdHandle, len(cmds))
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

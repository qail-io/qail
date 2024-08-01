// Package qail provides Go bindings for QAIL (Query Abstraction Interface Language).
//
// Example:
//
//	sql, err := qail.Transpile("get::users:'_", "postgres")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Println(sql) // SELECT * FROM "users"
package qail

/*
#cgo LDFLAGS: -L${SRCDIR} -lqail_ffi
#cgo darwin LDFLAGS: -Wl,-rpath,${SRCDIR}
#cgo linux LDFLAGS: -Wl,-rpath,${SRCDIR}

#include <stdlib.h>

extern char* qail_transpile(const char* qail);
extern char* qail_transpile_with_dialect(const char* qail, const char* dialect);
extern char* qail_parse_json(const char* qail);
extern int qail_validate(const char* qail);
extern const char* qail_last_error();
extern void qail_free(char* ptr);
extern char* qail_version();
*/
import "C"
import (
	"errors"
	"unsafe"
)

// Transpile converts QAIL to SQL for the specified dialect.
// dialect can be: "postgres", "mysql", "sqlite", "sqlserver"
func Transpile(qail, dialect string) (string, error) {
	cQail := C.CString(qail)
	cDialect := C.CString(dialect)
	defer C.free(unsafe.Pointer(cQail))
	defer C.free(unsafe.Pointer(cDialect))

	result := C.qail_transpile_with_dialect(cQail, cDialect)
	if result == nil {
		errMsg := C.qail_last_error()
		if errMsg != nil {
			return "", errors.New(C.GoString(errMsg))
		}
		return "", errors.New("unknown QAIL error")
	}
	defer C.qail_free(result)
	return C.GoString(result), nil
}

// ToSQL is an alias for Transpile with postgres dialect.
func ToSQL(qail string) (string, error) {
	return Transpile(qail, "postgres")
}

// ToPostgres transpiles QAIL to PostgreSQL.
func ToPostgres(qail string) (string, error) {
	return Transpile(qail, "postgres")
}

// ToMySQL transpiles QAIL to MySQL.
func ToMySQL(qail string) (string, error) {
	return Transpile(qail, "mysql")
}

// ToSQLite transpiles QAIL to SQLite.
func ToSQLite(qail string) (string, error) {
	return Transpile(qail, "sqlite")
}

// ParseJSON parses QAIL and returns the AST as JSON.
func ParseJSON(qail string) (string, error) {
	cQail := C.CString(qail)
	defer C.free(unsafe.Pointer(cQail))

	result := C.qail_parse_json(cQail)
	if result == nil {
		errMsg := C.qail_last_error()
		if errMsg != nil {
			return "", errors.New(C.GoString(errMsg))
		}
		return "", errors.New("unknown QAIL error")
	}
	defer C.qail_free(result)
	return C.GoString(result), nil
}

// Validate checks if QAIL syntax is valid.
func Validate(qail string) bool {
	cQail := C.CString(qail)
	defer C.free(unsafe.Pointer(cQail))
	return C.qail_validate(cQail) == 1
}

// Version returns the QAIL library version.
func Version() string {
	result := C.qail_version()
	if result == nil {
		return "unknown"
	}
	defer C.qail_free(result)
	return C.GoString(result)
}

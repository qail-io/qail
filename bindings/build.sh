#!/bin/bash
# Build QAIL FFI and copy to all binding directories

set -e

echo "=== Building QAIL FFI Library ==="
cd "$(dirname "$0")/.."

# Build release version
cargo build -p qail-ffi --release

# Determine library name based on OS
if [[ "$OSTYPE" == "darwin"* ]]; then
    LIB_NAME="libqail_ffi.dylib"
elif [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "win32" ]]; then
    LIB_NAME="qail_ffi.dll"
else
    LIB_NAME="libqail_ffi.so"
fi

LIB_PATH="target/release/$LIB_NAME"

echo "=== Copying $LIB_NAME to bindings ==="

# Copy to Python
cp "$LIB_PATH" bindings/qail-py/qail/
echo "  -> qail-py/qail/"

# Copy to Go
cp "$LIB_PATH" bindings/qail-go/
echo "  -> qail-go/"

# Copy to PHP
cp "$LIB_PATH" bindings/qail-php/
echo "  -> qail-php/"

# Copy to Java resources
cp "$LIB_PATH" bindings/qail-java/src/main/resources/
echo "  -> qail-java/src/main/resources/"

echo "=== Done! ==="
echo ""
echo "Language bindings are ready:"
echo "  Python: cd bindings/qail-py && pip install -e ."
echo "  Go:     export LD_LIBRARY_PATH=bindings/qail-go"
echo "  PHP:    include bindings/qail-php/src/Qail.php"
echo "  Java:   cd bindings/qail-java && mvn install"

// QAIL-WASM Benchmark
// Tests encoding/transpiling performance in Node.js

const fs = require('fs');
const path = require('path');

async function main() {
    // Load WASM module
    const wasmPath = path.join(__dirname, '../pkg/qail_wasm_bg.wasm');
    const jsPath = path.join(__dirname, '../pkg/qail_wasm.js');

    // Dynamic import for ESM
    const wasm = await import(jsPath);
    const wasmBuffer = fs.readFileSync(wasmPath);
    await wasm.default(wasmBuffer);

    console.log('🏁 QAIL-WASM BENCHMARK');
    console.log('======================');
    console.log(`Version: ${wasm.version()}`);
    console.log('');

    const ITERATIONS = 100000;

    // Test 1: Parse and transpile to SQL
    console.log('📊 Test 1: parse_and_transpile (QAIL → SQL)');
    const qailQuery = 'GET harbors | id, name | LIMIT 10';

    let start = Date.now();
    for (let i = 0; i < ITERATIONS; i++) {
        wasm.parse_and_transpile(qailQuery);
    }
    let elapsed = Date.now() - start;
    let opsPerSec = (ITERATIONS / elapsed) * 1000;
    console.log(`   ${ITERATIONS} iterations in ${elapsed}ms`);
    console.log(`   ${opsPerSec.toFixed(0)} ops/sec`);
    console.log(`   ${(elapsed * 1000 / ITERATIONS).toFixed(2)} µs/op`);
    console.log('');

    // Test 2: Parse only (to AST)
    console.log('📊 Test 2: parse (QAIL → AST JSON)');
    start = Date.now();
    for (let i = 0; i < ITERATIONS; i++) {
        wasm.parse(qailQuery);
    }
    elapsed = Date.now() - start;
    opsPerSec = (ITERATIONS / elapsed) * 1000;
    console.log(`   ${ITERATIONS} iterations in ${elapsed}ms`);
    console.log(`   ${opsPerSec.toFixed(0)} ops/sec`);
    console.log(`   ${(elapsed * 1000 / ITERATIONS).toFixed(2)} µs/op`);
    console.log('');

    // Test 3: Validate only
    console.log('📊 Test 3: validate (syntax check only)');
    start = Date.now();
    for (let i = 0; i < ITERATIONS; i++) {
        wasm.validate(qailQuery);
    }
    elapsed = Date.now() - start;
    opsPerSec = (ITERATIONS / elapsed) * 1000;
    console.log(`   ${ITERATIONS} iterations in ${elapsed}ms`);
    console.log(`   ${opsPerSec.toFixed(0)} ops/sec`);
    console.log(`   ${(elapsed * 1000 / ITERATIONS).toFixed(2)} µs/op`);
    console.log('');

    // Test 4: MongoDB transpile
    console.log('📊 Test 4: to_mongo (QAIL → MongoDB)');
    start = Date.now();
    for (let i = 0; i < ITERATIONS; i++) {
        wasm.to_mongo(qailQuery);
    }
    elapsed = Date.now() - start;
    opsPerSec = (ITERATIONS / elapsed) * 1000;
    console.log(`   ${ITERATIONS} iterations in ${elapsed}ms`);
    console.log(`   ${opsPerSec.toFixed(0)} ops/sec`);
    console.log(`   ${(elapsed * 1000 / ITERATIONS).toFixed(2)} µs/op`);
    console.log('');

    // Summary
    console.log('📈 SUMMARY:');
    console.log('┌────────────────────────────────────────┐');
    console.log('│ QAIL-WASM is encoding at:              │');
    console.log(`│ ${opsPerSec.toFixed(0).padStart(8)} ops/sec in WASM    │`);
    console.log('│ Compare: pgx ~250000 q/s (with I/O)    │');
    console.log('│ WASM is ENCODING only (no network)     │');
    console.log('└────────────────────────────────────────┘');
}

main().catch(console.error);

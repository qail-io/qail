// QAIL-WASM Benchmark (ESM)
// Tests encoding/transpiling performance in Node.js
//
// Run: node --experimental-wasm-modules bench.mjs

import init, { parse_and_transpile, parse, validate, to_mongo, version } from './pkg/qail_wasm.js';
import { readFileSync } from 'fs';

async function main() {
    // Initialize WASM
    const wasmBuffer = readFileSync('./pkg/qail_wasm_bg.wasm');
    await init(wasmBuffer);

    console.log('🏁 QAIL-WASM BENCHMARK');
    console.log('======================');
    console.log(`Version: ${version()}`);
    console.log('');

    const ITERATIONS = 100000;

    // Test 1: Parse and transpile to SQL
    console.log('📊 Test 1: parse_and_transpile (QAIL → SQL)');
    // Simplest form: action table
    const qailQuery = "get harbors";

    let start = Date.now();
    for (let i = 0; i < ITERATIONS; i++) {
        parse_and_transpile(qailQuery);
    }
    let elapsed = Date.now() - start;
    let opsPerSec = (ITERATIONS / elapsed) * 1000;
    let usPerOp = (elapsed * 1000 / ITERATIONS);
    console.log(`   ${ITERATIONS} iterations in ${elapsed}ms`);
    console.log(`   ${opsPerSec.toFixed(0)} ops/sec`);
    console.log(`   ${usPerOp.toFixed(2)} µs/op`);
    console.log('');

    const transpileOps = opsPerSec;

    // Test 2: Parse only (to AST)
    console.log('📊 Test 2: parse (QAIL → AST JSON)');
    start = Date.now();
    for (let i = 0; i < ITERATIONS; i++) {
        parse(qailQuery);
    }
    elapsed = Date.now() - start;
    opsPerSec = (ITERATIONS / elapsed) * 1000;
    usPerOp = (elapsed * 1000 / ITERATIONS);
    console.log(`   ${ITERATIONS} iterations in ${elapsed}ms`);
    console.log(`   ${opsPerSec.toFixed(0)} ops/sec`);
    console.log(`   ${usPerOp.toFixed(2)} µs/op`);
    console.log('');

    // Test 3: Validate only
    console.log('📊 Test 3: validate (syntax check only)');
    start = Date.now();
    for (let i = 0; i < ITERATIONS; i++) {
        validate(qailQuery);
    }
    elapsed = Date.now() - start;
    opsPerSec = (ITERATIONS / elapsed) * 1000;
    usPerOp = (elapsed * 1000 / ITERATIONS);
    console.log(`   ${ITERATIONS} iterations in ${elapsed}ms`);
    console.log(`   ${opsPerSec.toFixed(0)} ops/sec`);
    console.log(`   ${usPerOp.toFixed(2)} µs/op`);
    console.log('');

    const validateOps = opsPerSec;

    // Test 4: MongoDB transpile
    console.log('📊 Test 4: to_mongo (QAIL → MongoDB)');
    start = Date.now();
    for (let i = 0; i < ITERATIONS; i++) {
        to_mongo(qailQuery);
    }
    elapsed = Date.now() - start;
    opsPerSec = (ITERATIONS / elapsed) * 1000;
    usPerOp = (elapsed * 1000 / ITERATIONS);
    console.log(`   ${ITERATIONS} iterations in ${elapsed}ms`);
    console.log(`   ${opsPerSec.toFixed(0)} ops/sec`);
    console.log(`   ${usPerOp.toFixed(2)} µs/op`);
    console.log('');

    // Summary
    console.log('📈 SUMMARY:');
    console.log('┌────────────────────────────────────────┐');
    console.log(`│ Transpile: ${transpileOps.toFixed(0).padStart(8)} ops/sec     │`);
    console.log(`│ Validate:  ${validateOps.toFixed(0).padStart(8)} ops/sec     │`);
    console.log('├────────────────────────────────────────┤');
    console.log('│ For context:                           │');
    console.log('│ - pgx:     ~250,000 q/s (with I/O)     │');
    console.log('│ - Rust:    ~320,000 q/s (with I/O)     │');
    console.log('│ - WASM: encoding only (no network)     │');
    console.log('└────────────────────────────────────────┘');
}

main().catch(console.error);

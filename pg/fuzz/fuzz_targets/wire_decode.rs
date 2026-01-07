//! Fuzz target for BackendMessage::decode
//!
//! Goal: ensure `decode()` never panics on arbitrary input.
//! Any byte sequence should either parse successfully or return Err —
//! it should NEVER panic, abort, or enter an infinite loop.

#![no_main]
use libfuzzer_sys::fuzz_target;
use qail_pg::protocol::wire::BackendMessage;

fuzz_target!(|data: &[u8]| {
    // Primary target: the top-level decoder that handles all 20+ message types.
    // We don't care whether it succeeds or fails — only that it doesn't panic.
    let _ = BackendMessage::decode(data);

    // If decode succeeds, try decoding any remaining bytes (simulates a stream
    // of back-to-back messages, which is the real-world usage pattern).
    if let Ok((_msg, consumed)) = BackendMessage::decode(data) {
        if consumed < data.len() {
            let _ = BackendMessage::decode(&data[consumed..]);
        }
    }
});

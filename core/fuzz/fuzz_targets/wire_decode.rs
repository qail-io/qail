#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = qail_core::wire::decode_cmd_binary(data);
});

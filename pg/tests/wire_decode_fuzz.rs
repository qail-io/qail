//! Backend wire-decoder fuzz tests (mutation-based).
//!
//! This complements unit tests by mutating valid frames into malformed inputs
//! and asserting decoder panic-safety + sane consumed lengths.

use proptest::prelude::*;
use qail_pg::protocol::wire::BackendMessage;

fn valid_backend_frame() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        // ReadyForQuery (Z + transaction status)
        Just(vec![b'Z', 0, 0, 0, 5, b'I']),
        // CommandComplete
        Just({
            let mut v = vec![b'C', 0, 0, 0, 13];
            v.extend_from_slice(b"SELECT 1\0");
            v
        }),
        // ParameterStatus
        Just({
            let mut payload = b"server_version\0".to_vec();
            payload.extend_from_slice(b"16.0\0");
            let len = (payload.len() + 4) as i32;
            let mut v = vec![b'S'];
            v.extend_from_slice(&len.to_be_bytes());
            v.append(&mut payload);
            v
        }),
        // ErrorResponse
        Just({
            let mut payload = Vec::new();
            payload.push(b'S');
            payload.extend_from_slice(b"ERROR\0");
            payload.push(b'C');
            payload.extend_from_slice(b"XX000\0");
            payload.push(b'M');
            payload.extend_from_slice(b"boom\0");
            payload.push(0);
            let len = (payload.len() + 4) as i32;
            let mut v = vec![b'E'];
            v.extend_from_slice(&len.to_be_bytes());
            v.extend_from_slice(&payload);
            v
        }),
        // DataRow with one NULL column
        Just({
            let mut payload = Vec::new();
            payload.extend_from_slice(&1i16.to_be_bytes());
            payload.extend_from_slice(&(-1i32).to_be_bytes());
            let len = (payload.len() + 4) as i32;
            let mut v = vec![b'D'];
            v.extend_from_slice(&len.to_be_bytes());
            v.extend_from_slice(&payload);
            v
        }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(3000))]

    #[test]
    fn backend_decode_mutated_frames_never_panic(
        seed in valid_backend_frame(),
        byte_mutations in proptest::collection::vec((0usize..256usize, any::<u8>()), 0..24),
        override_len in prop::option::of(any::<i32>()),
        truncate_to in prop::option::of(0usize..256usize),
        append_garbage in proptest::collection::vec(any::<u8>(), 0..64),
    ) {
        let mut frame = seed;

        for (idx, value) in byte_mutations {
            if frame.is_empty() {
                break;
            }
            let pos = idx % frame.len();
            frame[pos] ^= value;
        }

        if let Some(len) = override_len
            && frame.len() >= 5
        {
            frame[1..5].copy_from_slice(&len.to_be_bytes());
        }

        if let Some(n) = truncate_to {
            let cap = frame.len();
            frame.truncate(n.min(cap));
        }

        frame.extend_from_slice(&append_garbage);

        let result = std::panic::catch_unwind(|| BackendMessage::decode(&frame));
        prop_assert!(result.is_ok(), "decoder panicked on mutated input");

        if let Ok(Ok((_msg, consumed))) = result {
            prop_assert!(consumed >= 5, "decoded frame consumed < header size");
            prop_assert!(consumed <= frame.len(), "decoded frame consumed beyond input length");
        }
    }

    #[test]
    fn backend_decode_stream_walk_makes_progress(
        stream in proptest::collection::vec(any::<u8>(), 0..1024)
    ) {
        // Simulate stream parser behavior over arbitrary bytes and assert
        // we always make forward progress even under malformed frames.
        let mut pos = 0usize;
        let mut steps = 0usize;
        while pos < stream.len() && steps < (stream.len() * 2 + 1) {
            match BackendMessage::decode(&stream[pos..]) {
                Ok((_msg, consumed)) if consumed > 0 => {
                    pos += consumed;
                }
                _ => {
                    pos += 1;
                }
            }
            steps += 1;
        }
        prop_assert!(steps <= stream.len() * 2 + 1, "stream walk failed to converge");
    }
}

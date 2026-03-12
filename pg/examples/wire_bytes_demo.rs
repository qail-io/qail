use qail_core::Qail;
use qail_core::ast::Operator;
use qail_pg::protocol::AstEncoder;

fn hex(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len() * 3);
    for (i, b) in data.iter().enumerate() {
        if i > 0 {
            out.push(' ');
        }
        use std::fmt::Write as _;
        let _ = write!(out, "{:02x}", b);
    }
    out
}

fn print_frames(wire: &[u8]) {
    let mut i = 0usize;
    let mut frame_idx = 0usize;

    while i + 5 <= wire.len() {
        let msg_type = wire[i] as char;
        let len = u32::from_be_bytes([wire[i + 1], wire[i + 2], wire[i + 3], wire[i + 4]]) as usize;
        let end = i + 1 + len;
        if end > wire.len() {
            println!(
                "Frame {} [{}]: invalid length {}, remaining {}",
                frame_idx,
                msg_type,
                len,
                wire.len().saturating_sub(i)
            );
            break;
        }

        let payload = &wire[i + 5..end];
        println!(
            "Frame {} [{}] total={} payload={} bytes",
            frame_idx,
            msg_type,
            1 + len,
            payload.len()
        );
        println!("  {}", hex(&wire[i..end]));

        i = end;
        frame_idx += 1;
    }
}

fn main() {
    // Sample from docs/question:
    // Qail::get("users").select_all().filter("active", Eq, true)
    let cmd = Qail::get("users")
        .select_all()
        .filter("active", Operator::Eq, true);

    // Human-inspection view (debug/tooling):
    let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).expect("encode_cmd_sql");
    println!("AST (Rust): Qail::get(\"users\").select_all().filter(\"active\", Eq, true)");
    println!("SQL view: {}", sql);
    println!("Bind params ({}):", params.len());
    for (idx, p) in params.iter().enumerate() {
        match p {
            None => println!("  ${}: NULL", idx + 1),
            Some(bytes) => println!("  ${}: [{}] {}", idx + 1, bytes.len(), hex(bytes)),
        }
    }

    // Actual wire bytes sent by encoder:
    let (wire, _) = AstEncoder::encode_cmd(&cmd).expect("encode_cmd");
    let wire = wire.freeze();

    println!("\nWire bytes ({} total):", wire.len());
    print_frames(&wire);

    println!("\nMessage flow: Parse (P) + Bind (B) + Describe (D) + Execute (E) + Sync (S)");
}

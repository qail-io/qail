fn main() {
    if let Err(err) = qail_mcp::run_stdio() {
        eprintln!("qail-mcp: {err}");
        std::process::exit(1);
    }
}

//! Test binary QAIL endpoint
//! Run with: cargo run --example test_binary

use qail_core::ast::Qail;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a QAIL query
    let cmd = Qail::get("harbors").columns(["id", "name"]).limit(3);
    
    // Serialize to bincode
    let bytes = bincode::serialize(&cmd)?;
    println!("Binary query size: {} bytes", bytes.len());
    
    // Send to gateway
    let client = reqwest::Client::new();
    let resp = client
        .post("http://localhost:8080/qail/binary")
        .header("Content-Type", "application/octet-stream")
        .body(bytes)
        .send()
        .await?;
    
    let status = resp.status();
    let body = resp.text().await?;
    
    println!("Status: {}", status);
    println!("Response: {}", body);
    
    Ok(())
}

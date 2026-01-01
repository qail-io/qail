//! Quick upsert debug test with delay
use qail_qdrant::{QdrantDriver, Point, PointId};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Connecting to Qdrant...");
    let mut driver = QdrantDriver::connect("localhost", 6334).await?;
    println!("Connected!");

    // Delete old test collection first
    let _ = driver.delete_collection("upsert_debug_test").await;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Create test collection
    println!("Creating test collection...");
    driver.create_collection(
        "upsert_debug_test",
        4,  // Small vector for testing
        qail_core::ast::Distance::Cosine,
        false,
    ).await?;
    println!("Collection created");
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Create simple test points
    let points = vec![
        Point {
            id: PointId::Num(1),
            vector: vec![0.1, 0.2, 0.3, 0.4],
            payload: std::collections::HashMap::new(),
        },
        Point {
            id: PointId::Num(2),
            vector: vec![0.5, 0.6, 0.7, 0.8],
            payload: std::collections::HashMap::new(),
        },
    ];

    println!("Upserting {} points with wait=true...", points.len());
    match driver.upsert("upsert_debug_test", &points, true).await {
        Ok(_) => println!("Upsert returned Ok()"),
        Err(e) => println!("Upsert error: {:?}", e),
    }

    // Wait for Qdrant to index
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    // Check via REST
    println!("\nChecking via REST...");
    let output = std::process::Command::new("curl")
        .args(["-s", "http://localhost:6333/collections/upsert_debug_test"])
        .output()?;
    let info = String::from_utf8_lossy(&output.stdout);
    println!("Collection info: {}", info);
    
    // Parse points_count
    if info.contains("\"points_count\":0") {
        println!("\n❌ Points count is still 0!");
    } else if info.contains("\"points_count\":") {
        println!("\n✓ Points were inserted!");
    }

    // Cleanup
    println!("\nCleaning up...");
    let _ = driver.delete_collection("upsert_debug_test").await;

    Ok(())
}

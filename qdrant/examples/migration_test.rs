use qail_qdrant::QdrantDriver;
use qail_core::ast::Distance;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut driver = QdrantDriver::connect("localhost", 6334).await?;
    let collection = "test_migration_collection";

    println!("Creating collection: {}", collection);
    driver.create_collection(collection, 128, Distance::Cosine, false).await?;
    println!("Collection created successfully!");

    // Wait a bit to ensure it's ready
    sleep(Duration::from_millis(500)).await;

    println!("Deleting collection: {}", collection);
    driver.delete_collection(collection).await?;
    println!("Collection deleted successfully!");

    Ok(())
}

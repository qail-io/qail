//! Qdrant snapshot operations via REST API.
//!
//! Provides backup and restore functionality for Qdrant collections.
//! Snapshots are REST-only (not gRPC) and stored as .tar archives.

use anyhow::Result;
use colored::*;
use reqwest::Client;
use serde::{Deserialize, Serialize};

/// Snapshot info returned by Qdrant.
#[derive(Debug, Deserialize)]
pub struct SnapshotInfo {
    pub name: String,
    pub creation_time: Option<String>,
    pub size: u64,
}

#[derive(Debug, Deserialize)]
struct SnapshotListResponse {
    result: Vec<SnapshotInfo>,
}

#[derive(Debug, Deserialize)]
struct SnapshotCreateResponse {
    result: SnapshotInfo,
}

#[derive(Debug, Serialize)]
struct SnapshotRecoverRequest {
    location: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    priority: Option<String>,
}

/// Create a snapshot of a collection.
pub async fn snapshot_create(collection: &str, url: &str) -> Result<SnapshotInfo> {
    println!("{} Creating snapshot of '{}'...", "→".cyan(), collection.yellow());

    let client = Client::new();
    let endpoint = format!("{}/collections/{}/snapshots", url, collection);

    let response = client
        .post(&endpoint)
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Snapshot creation failed: {} - {}", status, body);
    }

    let result: SnapshotCreateResponse = response.json().await?;
    println!("{} Snapshot created: {}", "✓".green(), result.result.name);
    
    Ok(result.result)
}

/// List snapshots for a collection.
pub async fn snapshot_list(collection: &str, url: &str) -> Result<Vec<SnapshotInfo>> {
    let client = Client::new();
    let endpoint = format!("{}/collections/{}/snapshots", url, collection);

    let response = client
        .get(&endpoint)
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Failed to list snapshots: {} - {}", status, body);
    }

    let result: SnapshotListResponse = response.json().await?;
    Ok(result.result)
}

/// Download a snapshot to a file.
pub async fn snapshot_download(collection: &str, snapshot_name: &str, output_path: &str, url: &str) -> Result<()> {
    println!("{} Downloading snapshot to '{}'...", "→".cyan(), output_path.yellow());

    let client = Client::new();
    let endpoint = format!("{}/collections/{}/snapshots/{}", url, collection, snapshot_name);

    let response = client
        .get(&endpoint)
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Snapshot download failed: {} - {}", status, body);
    }

    let bytes = response.bytes().await?;
    std::fs::write(output_path, &bytes)?;

    println!("{} Downloaded {} bytes to {}", "✓".green(), bytes.len(), output_path);
    Ok(())
}

/// Restore a collection from a snapshot.
pub async fn snapshot_restore(collection: &str, snapshot_location: &str, url: &str) -> Result<()> {
    println!("{} Restoring '{}' from snapshot...", "→".cyan(), collection.yellow());
    println!("  Location: {}", snapshot_location);

    let client = Client::new();
    let endpoint = format!("{}/collections/{}/snapshots/recover", url, collection);

    let request = SnapshotRecoverRequest {
        location: snapshot_location.to_string(),
        priority: Some("snapshot".to_string()), // Prefer snapshot data
    };

    let response = client
        .put(&endpoint)
        .json(&request)
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Snapshot restore failed: {} - {}", status, body);
    }

    println!("{} Collection '{}' restored successfully!", "✓".green(), collection);
    Ok(())
}

/// Delete a snapshot.
pub async fn snapshot_delete(collection: &str, snapshot_name: &str, url: &str) -> Result<()> {
    let client = Client::new();
    let endpoint = format!("{}/collections/{}/snapshots/{}", url, collection, snapshot_name);

    let response = client
        .delete(&endpoint)
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Snapshot deletion failed: {} - {}", status, body);
    }

    println!("{} Snapshot '{}' deleted", "✓".green(), snapshot_name);
    Ok(())
}

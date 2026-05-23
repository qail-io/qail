//! Qdrant snapshot operations via REST API.
//!
//! Provides backup and restore functionality for Qdrant collections.
//! Snapshots are REST-only (not gRPC) and stored as .tar archives.

use crate::colors::*;
use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use url::Url;

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
    println!(
        "{} Creating snapshot of '{}'...",
        "→".cyan(),
        collection.yellow()
    );

    let client = Client::new();
    let endpoint = qdrant_endpoint(url, &["collections", collection, "snapshots"])?;

    let response = client.post(&endpoint).send().await?;

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
    let endpoint = qdrant_endpoint(url, &["collections", collection, "snapshots"])?;

    let response = client.get(&endpoint).send().await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Failed to list snapshots: {} - {}", status, body);
    }

    let result: SnapshotListResponse = response.json().await?;
    Ok(result.result)
}

/// Download a snapshot to a local file.
///
/// # Arguments
///
/// * `collection` — Qdrant collection name.
/// * `snapshot_name` — Name of the snapshot to download.
/// * `output_path` — Local file path to write the snapshot archive to.
/// * `url` — Qdrant REST API base URL.
pub async fn snapshot_download(
    collection: &str,
    snapshot_name: &str,
    output_path: &str,
    url: &str,
) -> Result<()> {
    println!(
        "{} Downloading snapshot to '{}'...",
        "→".cyan(),
        output_path.yellow()
    );

    let client = Client::new();
    let endpoint = qdrant_endpoint(
        url,
        &["collections", collection, "snapshots", snapshot_name],
    )?;

    let mut response = client.get(&endpoint).send().await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Snapshot download failed: {} - {}", status, body);
    }

    let mut file = tokio::fs::File::create(output_path).await?;
    let mut bytes_written: u64 = 0;
    while let Some(chunk) = response.chunk().await? {
        file.write_all(&chunk).await?;
        bytes_written += chunk.len() as u64;
    }
    file.flush().await?;

    println!(
        "{} Downloaded {} bytes to {}",
        "✓".green(),
        bytes_written,
        output_path
    );
    Ok(())
}

/// Restore a collection from a snapshot.
pub async fn snapshot_restore(collection: &str, snapshot_location: &str, url: &str) -> Result<()> {
    println!(
        "{} Restoring '{}' from snapshot...",
        "→".cyan(),
        collection.yellow()
    );
    println!("  Location: {}", snapshot_location);

    let client = Client::new();
    let endpoint = qdrant_endpoint(url, &["collections", collection, "snapshots", "recover"])?;

    let request = SnapshotRecoverRequest {
        location: snapshot_location.to_string(),
        priority: Some("snapshot".to_string()), // Prefer snapshot data
    };

    let response = client.put(&endpoint).json(&request).send().await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Snapshot restore failed: {} - {}", status, body);
    }

    println!(
        "{} Collection '{}' restored successfully!",
        "✓".green(),
        collection
    );
    Ok(())
}

/// Delete a snapshot.
pub async fn snapshot_delete(collection: &str, snapshot_name: &str, url: &str) -> Result<()> {
    let client = Client::new();
    let endpoint = qdrant_endpoint(
        url,
        &["collections", collection, "snapshots", snapshot_name],
    )?;

    let response = client.delete(&endpoint).send().await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Snapshot deletion failed: {} - {}", status, body);
    }

    println!("{} Snapshot '{}' deleted", "✓".green(), snapshot_name);
    Ok(())
}

fn qdrant_endpoint(base_url: &str, segments: &[&str]) -> Result<String> {
    let mut url = Url::parse(base_url).context("Invalid Qdrant URL")?;
    {
        let mut path = url
            .path_segments_mut()
            .map_err(|_| anyhow::anyhow!("Qdrant URL cannot be used as a path base"))?;
        path.pop_if_empty();
        for segment in segments {
            path.push(segment);
        }
    }
    Ok(url.to_string())
}

#[cfg(test)]
mod tests {
    use super::qdrant_endpoint;

    #[test]
    fn qdrant_endpoint_encodes_collection_and_snapshot_segments() {
        let endpoint = qdrant_endpoint(
            "http://localhost:6333",
            &["collections", "tenant/a?x", "snapshots", "snap 1.tar"],
        )
        .unwrap();

        assert_eq!(
            endpoint,
            "http://localhost:6333/collections/tenant%2Fa%3Fx/snapshots/snap%201.tar"
        );
    }

    #[test]
    fn qdrant_endpoint_preserves_base_path_without_double_slash() {
        let endpoint = qdrant_endpoint(
            "http://localhost:6333/api/",
            &["collections", "products", "snapshots"],
        )
        .unwrap();

        assert_eq!(
            endpoint,
            "http://localhost:6333/api/collections/products/snapshots"
        );
    }
}

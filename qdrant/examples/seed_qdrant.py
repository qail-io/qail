#!/usr/bin/env python3
"""
Seed Qdrant with realistic complex data for benchmarking.
Run: python3 seed_qdrant.py
Requires: pip install qdrant-client numpy
"""

from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
import numpy as np

# Config
COLLECTION_NAME = "benchmark_collection"
VECTOR_DIM = 1536  # OpenAI embedding dimension
NUM_POINTS = 1000
HOST = "localhost"
PORT = 6333

print("🔌 Connecting to Qdrant...")
client = QdrantClient(url=f"http://{HOST}:{PORT}")

# Cleanup
print(f"🗑️  Deleting existing collection '{COLLECTION_NAME}' (if exists)...")
try:
    client.delete_collection(COLLECTION_NAME)
except:
    pass

# Create collection
print(f"📦 Creating collection '{COLLECTION_NAME}' ({VECTOR_DIM} dimensions)...")
client.create_collection(
    collection_name=COLLECTION_NAME,
    vectors_config=VectorParams(size=VECTOR_DIM, distance=Distance.COSINE),
)

# Generate realistic data
print(f"📊 Generating {NUM_POINTS} realistic points with metadata...")
categories = ["electronics", "books", "clothing", "home", "sports"]
brands = ["Apple", "Samsung", "Sony", "Amazon", "Nike"]

points = []
for i in range(NUM_POINTS):
    # Generate realistic normalized embeddings (simulating sentence transformers)
    seed_base = i * 31
    vector = np.array([
        np.sin(seed_base + j * 17) * 0.5 +
        np.cos((seed_base + j * 17) / 100.0) * 0.3 +
        np.sin((seed_base + j * 17) / 1000.0) * 0.2
        for j in range(VECTOR_DIM)
    ], dtype=np.float32)
    
    # Normalize (L2 norm)
    norm = np.linalg.norm(vector)
    if norm > 0:
        vector = vector / norm
    
    # Create point with complex payload
    point = PointStruct(
        id=i,
        vector=vector.tolist(),
        payload={
            "product_id": i,
            "category": categories[i % len(categories)],
            "brand": brands[i % len(brands)],
            "price": (i % 100) + 10,
            "rating": (i % 50) / 10.0,
            "in_stock": (i % 3) != 0,
        }
    )
    points.append(point)

# Upsert in batches
BATCH_SIZE = 100
print(f"💾 Upserting {NUM_POINTS} points in batches of {BATCH_SIZE}...")
for i in range(0, NUM_POINTS, BATCH_SIZE):
    batch = points[i:i+BATCH_SIZE]
    client.upsert(collection_name=COLLECTION_NAME, points=batch)
    print(f"   ✓ Uploaded {min(i+BATCH_SIZE, NUM_POINTS)}/{NUM_POINTS} points")

print(f"\n✅ Seed complete! Collection '{COLLECTION_NAME}' ready for benchmarking.")
print(f"   Run: cd /Users/orion/qail.rs && cargo run --example qdrant_benchmark --release")

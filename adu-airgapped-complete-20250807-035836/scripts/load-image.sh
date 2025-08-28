#!/bin/bash

# Load ADU Export Docker Image
# Run this script to load the Docker image in your air-gapped environment

set -e

echo "🚀 Loading ADU Export Docker Image"
echo "================================="
echo ""

# Check if Docker is installed and running
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

if ! docker info &> /dev/null; then
    echo "❌ Docker daemon is not running. Please start Docker service."
    exit 1
fi

# Find the image file
IMAGE_FILE=""
for file in ../adu-export-image.tar adu-export-image.tar; do
    if [ -f "$file" ]; then
        IMAGE_FILE="$file"
        break
    fi
done

if [ -z "$IMAGE_FILE" ]; then
    echo "❌ Image file not found. Expected: adu-export-image.tar"
    exit 1
fi

echo "📦 Loading image from: $IMAGE_FILE"
IMAGE_SIZE=$(du -h "$IMAGE_FILE" | cut -f1)
echo "   Image size: $IMAGE_SIZE"
echo ""

# Load the image
docker load -i "$IMAGE_FILE"

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Image loaded successfully!"
    echo ""
    echo "📋 Available ADU images:"
    docker images | grep adu-export
    echo ""
    echo "🎯 Next step: Run './run-container.sh' to start the application"
else
    echo "❌ Failed to load image"
    exit 1
fi

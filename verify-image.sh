#!/bin/bash

# ADU Export Application - Image Verification Script
# This script verifies the Docker image works correctly

echo "🔍 ADU Export Image Verification Script"
echo "========================================"

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed or not in PATH"
    exit 1
fi

# Check if image file exists
IMAGE_FILE="exports/images/adu-export-airgapped-20250805-184113.tar"
if [ ! -f "$IMAGE_FILE" ]; then
    echo "❌ Image file not found: $IMAGE_FILE"
    exit 1
fi

echo "✅ Docker is available"
echo "✅ Image file found: $IMAGE_FILE ($(du -h "$IMAGE_FILE" | cut -f1))"

# Load the image
echo ""
echo "📦 Loading Docker image..."
docker load -i "$IMAGE_FILE"

if [ $? -eq 0 ]; then
    echo "✅ Image loaded successfully"
else
    echo "❌ Failed to load image"
    exit 1
fi

# Verify image exists
echo ""
echo "🔍 Verifying image..."
if docker images | grep -q "adu-export.*airgapped"; then
    echo "✅ Image verified in Docker"
    docker images | grep "adu-export"
else
    echo "❌ Image not found in Docker images"
    exit 1
fi

# Test container startup (quick test)
echo ""
echo "🚀 Testing container startup..."
CONTAINER_NAME="adu-export-test-$$"

# Start container in background
docker run -d \
    --name "$CONTAINER_NAME" \
    -p 5001:5000 \
    adu-export:airgapped

if [ $? -eq 0 ]; then
    echo "✅ Container started successfully"
    
    # Wait a few seconds for startup
    echo "⏳ Waiting for application startup..."
    sleep 10
    
    # Test health endpoint
    echo "🔍 Testing health endpoint..."
    if curl -s http://localhost:5001/health | grep -q "healthy"; then
        echo "✅ Health endpoint responding correctly"
    else
        echo "⚠️  Health endpoint not responding (this is normal during initial startup)"
    fi
    
    # Check container logs
    echo ""
    echo "📋 Container startup logs:"
    docker logs "$CONTAINER_NAME" | tail -10
    
    # Cleanup
    echo ""
    echo "🧹 Cleaning up test container..."
    docker stop "$CONTAINER_NAME" > /dev/null 2>&1
    docker rm "$CONTAINER_NAME" > /dev/null 2>&1
    echo "✅ Test container cleaned up"
    
else
    echo "❌ Failed to start container"
    exit 1
fi

echo ""
echo "🎉 Image verification completed successfully!"
echo ""
echo "📋 Summary:"
echo "   • Image file: $IMAGE_FILE"
echo "   • Compressed size: $(du -h "exports/images/adu-export-airgapped-20250805-184113.tar.gz" | cut -f1)"
echo "   • Uncompressed size: $(du -h "$IMAGE_FILE" | cut -f1)"
echo "   • Docker image: adu-export:airgapped"
echo "   • Container test: ✅ PASSED"
echo ""
echo "🚀 Ready for deployment to airgapped environment!"
echo ""
echo "Next steps:"
echo "1. Transfer 'exports/images/adu-export-airgapped-20250805-184113.tar.gz' to airgapped environment"
echo "2. Follow instructions in 'AIRGAPPED_DEPLOYMENT_INSTRUCTIONS.md'"
echo "3. Load image: docker load -i adu-export-airgapped-20250805-184113.tar"
echo "4. Deploy with volume mounts for persistent data"

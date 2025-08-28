#!/bin/bash

# Production build script for ADU Export Application with Gunicorn
# This script builds and optionally exports the production Docker image

set -e  # Exit on any error

echo "=== Building ADU Export Application - Production Image ==="

# Default values
EXPORT_IMAGE=false
PUSH_IMAGE=false
IMAGE_TAG="export-app:production"
EXPORT_DIR="exports"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --export)
            EXPORT_IMAGE=true
            shift
            ;;
        --push)
            PUSH_IMAGE=true
            shift
            ;;
        --tag)
            IMAGE_TAG="$2"
            shift 2
            ;;
        --export-dir)
            EXPORT_DIR="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --export         Export image as tar file after building"
            echo "  --push           Push image to registry after building"
            echo "  --tag TAG        Use custom image tag (default: export-app:production)"
            echo "  --export-dir DIR Export directory (default: exports)"
            echo "  -h, --help       Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

echo "Configuration:"
echo "  - Image Tag: $IMAGE_TAG"
echo "  - Export Image: $EXPORT_IMAGE"
echo "  - Push Image: $PUSH_IMAGE"
echo "  - Export Directory: $EXPORT_DIR"
echo ""

# Build the production image
echo "Building production Docker image..."
docker build -t "$IMAGE_TAG" .

if [ $? -eq 0 ]; then
    echo "✅ Build completed successfully!"
    
    # Get image size
    IMAGE_SIZE=$(docker images "$IMAGE_TAG" --format "{{.Size}}")
    echo "📦 Image size: $IMAGE_SIZE"
else
    echo "❌ Build failed!"
    exit 1
fi

# Export image if requested
if [ "$EXPORT_IMAGE" = true ]; then
    echo ""
    echo "Exporting Docker image..."
    
    # Create export directory if it doesn't exist
    mkdir -p "$EXPORT_DIR"
    
    # Generate filename with timestamp
    TIMESTAMP=$(date +%Y%m%d-%H%M%S)
    EXPORT_FILE="$EXPORT_DIR/export-app-gunicorn-production-$TIMESTAMP.tar"
    
    docker save -o "$EXPORT_FILE" "$IMAGE_TAG"
    
    if [ $? -eq 0 ]; then
        echo "✅ Image exported to: $EXPORT_FILE"
        
        # Show file size
        FILE_SIZE=$(ls -lh "$EXPORT_FILE" | awk '{print $5}')
        echo "📁 File size: $FILE_SIZE"
        
        echo ""
        echo "To import in air-gapped environment:"
        echo "  docker load -i $EXPORT_FILE"
    else
        echo "❌ Export failed!"
        exit 1
    fi
fi

# Push image if requested
if [ "$PUSH_IMAGE" = true ]; then
    echo ""
    echo "Pushing Docker image..."
    docker push "$IMAGE_TAG"
    
    if [ $? -eq 0 ]; then
        echo "✅ Image pushed successfully!"
    else
        echo "❌ Push failed!"
        exit 1
    fi
fi

echo ""
echo "=== Production Build Complete ==="
echo ""
echo "To run the production container:"
echo "  docker run --rm -p 5000:5000 $IMAGE_TAG"
echo ""
echo "To run with custom environment:"
echo "  docker run --rm -p 5000:5000 -e GUNICORN_WORKERS=8 -e PORT=8080 $IMAGE_TAG"

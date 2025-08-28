#!/bin/bash

# Run ADU Export Application Container
# Starts the application without Docker Compose

set -e

echo "🚀 Starting ADU Export Application"
echo "================================="
echo ""

# Configuration
CONTAINER_NAME="adu-export-app"
HOST_PORT="8080"
HOST_EXPORTS_DIR="$(pwd)/../runtime/exports"
HOST_DATA_DIR="$(pwd)/../runtime/data"

# Check if Docker is running
if ! docker info &> /dev/null; then
    echo "❌ Docker daemon is not running. Please start Docker service."
    exit 1
fi

# Find the ADU image
ADU_IMAGE=$(docker images --format "table {{.Repository}}:{{.Tag}}" | grep adu-export | head -1 | tr -d ' ')

if [ -z "$ADU_IMAGE" ]; then
    echo "❌ No ADU export image found. Please run './load-image.sh' first."
    exit 1
fi

echo "📋 Configuration:"
echo "   Image: $ADU_IMAGE"
echo "   Container: $CONTAINER_NAME"
echo "   Web Port: http://localhost:$HOST_PORT"
echo "   Exports Directory: $HOST_EXPORTS_DIR"
echo "   Data Directory: $HOST_DATA_DIR"
echo ""

# Create directories
echo "📁 Creating host directories..."
mkdir -p "$HOST_EXPORTS_DIR"
mkdir -p "$HOST_DATA_DIR"

# Stop existing container if running
if docker ps -a --format "{{.Names}}" | grep -q "^${CONTAINER_NAME}$"; then
    echo "🛑 Stopping existing container..."
    docker stop "$CONTAINER_NAME" >/dev/null 2>&1 || true
    docker rm "$CONTAINER_NAME" >/dev/null 2>&1 || true
fi

# Start the container
echo "🚀 Starting container..."
docker run -d \
    --name "$CONTAINER_NAME" \
    --restart unless-stopped \
    -p "$HOST_PORT:8080" \
    -v "$HOST_EXPORTS_DIR:/app/exports" \
    -v "$HOST_DATA_DIR:/app/adu/data" \
    -e "FLASK_ENV=production" \
    -e "PYTHONUNBUFFERED=1" \
    "$ADU_IMAGE"

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Container started successfully!"
    echo ""
    echo "🌐 Application URLs:"
    echo "   Main Application: http://localhost:$HOST_PORT"
    echo "   Job History: http://localhost:$HOST_PORT/history"
    echo "   Worker Logs: http://localhost:$HOST_PORT/logs"
    echo ""
    echo "📁 Host Directories:"
    echo "   Exports: $HOST_EXPORTS_DIR"
    echo "   Database: $HOST_DATA_DIR"
    echo ""
    echo "🔧 Management Commands:"
    echo "   View logs: docker logs $CONTAINER_NAME"
    echo "   Stop app: docker stop $CONTAINER_NAME"
    echo "   Restart: docker restart $CONTAINER_NAME"
    echo ""
    
    # Wait a moment and check if container is still running
    sleep 3
    if docker ps --format "{{.Names}}" | grep -q "^${CONTAINER_NAME}$"; then
        echo "✅ Container is running healthy"
        
        # Show recent logs
        echo ""
        echo "📋 Recent startup logs:"
        docker logs --tail 10 "$CONTAINER_NAME"
    else
        echo "❌ Container failed to start. Checking logs..."
        docker logs "$CONTAINER_NAME"
        exit 1
    fi
else
    echo "❌ Failed to start container"
    exit 1
fi

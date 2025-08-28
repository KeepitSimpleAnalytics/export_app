#!/bin/bash

# ADU Export Application - Airgapped Deployment Script v2.0
# This script automates the deployment process in airgapped environments

set -e  # Exit on any error

echo "🚀 ADU Export Application - Airgapped Deployment v2.0"
echo "======================================================"
echo ""

# Configuration
IMAGE_FILE="adu-airgapped-v2-20250806-173019.tar"
CONTAINER_NAME="adu-export-prod"
DATA_DIR="/data/adu-export"
WEB_PORT="5000"

# Check if running as root (for directory creation)
if [[ $EUID -ne 0 ]]; then
   echo "⚠️  This script requires root privileges for directory creation"
   echo "   Please run with sudo or as root user"
   exit 1
fi

# Function to check prerequisites
check_prerequisites() {
    echo "🔍 Checking prerequisites..."
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        echo "❌ Docker is not installed. Please install Docker first."
        exit 1
    fi
    echo "✅ Docker is available: $(docker --version)"
    
    # Check if Docker daemon is running
    if ! docker info &> /dev/null; then
        echo "❌ Docker daemon is not running. Please start Docker service."
        exit 1
    fi
    echo "✅ Docker daemon is running"
    
    # Check image file
    if [ ! -f "$IMAGE_FILE" ]; then
        echo "❌ Image file not found: $IMAGE_FILE"
        echo "   Please ensure the image file is in the current directory"
        exit 1
    fi
    echo "✅ Image file found: $IMAGE_FILE ($(du -h "$IMAGE_FILE" | cut -f1))"
    
    echo ""
}

# Function to create directories
create_directories() {
    echo "📁 Creating data directories..."
    
    mkdir -p "$DATA_DIR"/{exports,database,logs,temp}
    
    # Set permissions
    chmod 755 "$DATA_DIR"
    chmod 755 "$DATA_DIR"/{exports,database,logs,temp}
    
    echo "✅ Created directories:"
    echo "   $DATA_DIR/exports   - Export output files"
    echo "   $DATA_DIR/database  - SQLite database"
    echo "   $DATA_DIR/logs      - Application logs"
    echo "   $DATA_DIR/temp      - Temporary files"
    echo ""
}

# Function to load Docker image
load_image() {
    echo "📦 Loading Docker image..."
    
    docker load -i "$IMAGE_FILE"
    
    if [ $? -eq 0 ]; then
        echo "✅ Image loaded successfully"
    else
        echo "❌ Failed to load image"
        exit 1
    fi
    
    # Verify image
    if docker images | grep -q "adu-export.*airgapped"; then
        echo "✅ Image verified in Docker:"
        docker images | grep "adu-export"
        echo ""
    else
        echo "❌ Image not found in Docker images"
        exit 1
    fi
}

# Function to stop existing container
stop_existing() {
    echo "🔄 Checking for existing container..."
    
    if docker ps -a | grep -q "$CONTAINER_NAME"; then
        echo "⚠️  Existing container found: $CONTAINER_NAME"
        read -p "   Stop and remove existing container? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            docker stop "$CONTAINER_NAME" 2>/dev/null || true
            docker rm "$CONTAINER_NAME" 2>/dev/null || true
            echo "✅ Existing container removed"
        else
            echo "❌ Deployment cancelled"
            exit 1
        fi
    fi
    echo ""
}

# Function to deploy container
deploy_container() {
    echo "🚀 Deploying container..."
    
    # Get system info for optimal configuration
    CPU_CORES=$(nproc)
    TOTAL_RAM_KB=$(grep MemTotal /proc/meminfo | awk '{print $2}')
    TOTAL_RAM_GB=$((TOTAL_RAM_KB / 1024 / 1024))
    
    echo "📊 System Resources:"
    echo "   CPU Cores: $CPU_CORES"
    echo "   Total RAM: ${TOTAL_RAM_GB}GB"
    
    # Determine configuration based on available resources
    if [ "$TOTAL_RAM_GB" -ge 32 ] && [ "$CPU_CORES" -ge 16 ]; then
        echo "🎯 Using HIGH-PERFORMANCE configuration"
        MEMORY_LIMIT="32g"
        MEMORY_RESERVATION="16g" 
        CPU_LIMIT="$CPU_CORES"
        MAX_THREADS="$CPU_CORES"
    elif [ "$TOTAL_RAM_GB" -ge 16 ] && [ "$CPU_CORES" -ge 8 ]; then
        echo "🎯 Using STANDARD configuration"
        MEMORY_LIMIT="16g"
        MEMORY_RESERVATION="8g"
        CPU_LIMIT="$CPU_CORES"
        MAX_THREADS="$CPU_CORES"
    else
        echo "🎯 Using BASIC configuration"
        MEMORY_LIMIT="8g"
        MEMORY_RESERVATION="4g"
        CPU_LIMIT="$CPU_CORES"
        MAX_THREADS="$CPU_CORES"
    fi
    
    echo "🔧 Container Configuration:"
    echo "   Memory Limit: $MEMORY_LIMIT"
    echo "   Memory Reservation: $MEMORY_RESERVATION"
    echo "   CPU Limit: $CPU_LIMIT"
    echo "   Max Threads: $MAX_THREADS"
    echo ""
    
    # Deploy container
    docker run -d \
        --name "$CONTAINER_NAME" \
        --restart unless-stopped \
        -p "$WEB_PORT:5000" \
        \
        -e OMP_NUM_THREADS="$MAX_THREADS" \
        -e POLARS_MAX_THREADS="$MAX_THREADS" \
        -e FLASK_DEBUG=False \
        -e PYTHONPATH=/app \
        -e MAX_CHUNK_WORKERS=4 \
        \
        --memory="$MEMORY_LIMIT" \
        --memory-reservation="$MEMORY_RESERVATION" \
        --cpus="$CPU_LIMIT" \
        --oom-kill-disable=false \
        \
        -v "$DATA_DIR/exports:/app/exports" \
        -v "$DATA_DIR/database:/app/adu/data" \
        -v "$DATA_DIR/logs:/app/logs" \
        -v "$DATA_DIR/temp:/tmp" \
        \
        adu-export:airgapped
    
    if [ $? -eq 0 ]; then
        echo "✅ Container deployed successfully"
    else
        echo "❌ Failed to deploy container"
        exit 1
    fi
    echo ""
}

# Function to verify deployment
verify_deployment() {
    echo "🔍 Verifying deployment..."
    
    # Wait for startup
    echo "⏳ Waiting for application startup..."
    sleep 10
    
    # Check container status
    if docker ps | grep -q "$CONTAINER_NAME"; then
        echo "✅ Container is running"
    else
        echo "❌ Container is not running"
        echo "📋 Container logs:"
        docker logs "$CONTAINER_NAME"
        exit 1
    fi
    
    # Check web interface
    echo "🌐 Testing web interface..."
    for i in {1..6}; do
        if curl -s "http://localhost:$WEB_PORT/" > /dev/null; then
            echo "✅ Web interface is accessible at http://localhost:$WEB_PORT"
            break
        elif [ $i -eq 6 ]; then
            echo "❌ Web interface is not accessible after 30 seconds"
            echo "📋 Container logs:"
            docker logs "$CONTAINER_NAME" --tail 20
            exit 1
        else
            echo "   Attempt $i/6... waiting 5 seconds"
            sleep 5
        fi
    done
    echo ""
}

# Function to show final information
show_final_info() {
    echo "🎉 Deployment completed successfully!"
    echo ""
    echo "📱 Access Information:"
    echo "   Web Interface: http://localhost:$WEB_PORT"
    echo "   Container Name: $CONTAINER_NAME"
    echo "   Data Directory: $DATA_DIR"
    echo ""
    echo "🔧 Management Commands:"
    echo "   View logs:     docker logs -f $CONTAINER_NAME"
    echo "   Stop:          docker stop $CONTAINER_NAME"
    echo "   Start:         docker start $CONTAINER_NAME"
    echo "   Restart:       docker restart $CONTAINER_NAME"
    echo "   Stats:         docker stats $CONTAINER_NAME"
    echo ""
    echo "📊 Performance Monitoring:"
    echo "   System stats:  docker stats $CONTAINER_NAME"
    echo "   Worker logs:   docker exec $CONTAINER_NAME tail -f /tmp/worker.log"
    echo "   Web logs:      docker logs $CONTAINER_NAME"
    echo ""
    echo "🚀 Ready for large dataset processing with v2.0 optimizations!"
}

# Main execution
main() {
    check_prerequisites
    create_directories
    load_image
    stop_existing
    deploy_container
    verify_deployment
    show_final_info
}

# Run main function
main "$@"

#!/bin/bash

# ADU Docker Stop Script
# Gracefully stops and manages the ADU container

set -e

# Configuration
CONTAINER_NAME="adu-export"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo "🛑 ADU Container Stop Script"
echo "============================"
echo ""

# Function to log with color
log() {
    echo -e "${BLUE}[$(date '+%H:%M:%S')]${NC} $1"
}

success() {
    echo -e "${GREEN}✅ $1${NC}"
}

warning() {
    echo -e "${YELLOW}⚠️ $1${NC}"
}

error() {
    echo -e "${RED}❌ $1${NC}"
}

# Function to check if container exists
container_exists() {
    docker ps -a --format "{{.Names}}" | grep -q "^${CONTAINER_NAME}$"
}

# Function to check if container is running
container_running() {
    docker ps --format "{{.Names}}" | grep -q "^${CONTAINER_NAME}$"
}

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    error "Docker is not installed or not in PATH"
fi

# Check if container exists
if ! container_exists; then
    warning "Container ${CONTAINER_NAME} does not exist"
    exit 0
fi

# Check container status
if container_running; then
    log "Container ${CONTAINER_NAME} is running, stopping gracefully..."
    
    # Try graceful shutdown first
    log "Attempting graceful shutdown..."
    docker exec "$CONTAINER_NAME" supervisorctl stop all 2>/dev/null || warning "Could not stop services gracefully"
    
    sleep 5
    
    # Stop the container
    docker stop "$CONTAINER_NAME"
    
    if container_running; then
        warning "Container did not stop gracefully, forcing stop..."
        docker kill "$CONTAINER_NAME"
    fi
    
    success "Container stopped"
else
    log "Container ${CONTAINER_NAME} is already stopped"
fi

# Show options for cleanup
echo ""
echo "📋 Next Steps:"
echo "=============="
echo ""
echo "• Start container: ./docker-run.sh"
echo "• Remove container: docker rm ${CONTAINER_NAME}"
echo "• View logs: docker logs ${CONTAINER_NAME}"
echo ""

# Parse command line arguments
if [ "$1" = "--remove" ] || [ "$1" = "-r" ]; then
    log "Removing stopped container..."
    docker rm "$CONTAINER_NAME"
    success "Container removed"
elif [ "$1" = "--clean" ] || [ "$1" = "-c" ]; then
    log "Removing container and cleaning up..."
    docker rm "$CONTAINER_NAME" 2>/dev/null || true
    
    # Clean up logs (optional)
    if [ -d "./logs" ]; then
        log "Cleaning up logs..."
        find ./logs -name "*.log" -type f -delete 2>/dev/null || true
        success "Logs cleaned"
    fi
    
    success "Container and logs cleaned up"
fi

success "ADU container stop completed"
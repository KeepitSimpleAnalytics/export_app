#!/bin/bash

# ADU Docker Run Script - Simple Single Container Deployment
# No Docker Compose required - perfect for air-gapped environments

set -e

# Configuration
IMAGE_NAME="adu-high-performance"
IMAGE_TAG="latest"
CONTAINER_NAME="adu-export"
HOST_PORT="8501"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo "🐳 ADU High-Performance Container Deployment"
echo "============================================"
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
    exit 1
}

# Function to check if container exists
container_exists() {
    docker ps -a --format "{{.Names}}" | grep -q "^${CONTAINER_NAME}$"
}

# Function to check if container is running
container_running() {
    docker ps --format "{{.Names}}" | grep -q "^${CONTAINER_NAME}$"
}

# Pre-run checks
log "Running pre-deployment checks..."

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    error "Docker is not installed or not in PATH"
fi

# Check Docker daemon
if ! docker info &> /dev/null; then
    error "Docker daemon is not running"
fi

# Check if image exists
if ! docker images --format "{{.Repository}}:{{.Tag}}" | grep -q "^${IMAGE_NAME}:${IMAGE_TAG}$"; then
    error "Docker image ${IMAGE_NAME}:${IMAGE_TAG} not found. Run ./docker-build.sh first."
fi

# Check if port is available
if netstat -tuln 2>/dev/null | grep -q ":${HOST_PORT} " || ss -tuln 2>/dev/null | grep -q ":${HOST_PORT} "; then
    warning "Port ${HOST_PORT} appears to be in use. Container may fail to start."
fi

success "Pre-deployment checks completed"

# Handle existing container
if container_exists; then
    if container_running; then
        log "Container ${CONTAINER_NAME} is already running"
        echo ""
        echo "Options:"
        echo "1. Stop and restart: ./docker-stop.sh && ./docker-run.sh"
        echo "2. View logs: docker logs -f ${CONTAINER_NAME}"
        echo "3. Access shell: docker exec -it ${CONTAINER_NAME} /bin/bash"
        echo ""
        echo "🌐 Access application at: http://localhost:${HOST_PORT}"
        exit 0
    else
        log "Removing stopped container ${CONTAINER_NAME}..."
        docker rm "$CONTAINER_NAME"
        success "Stopped container removed"
    fi
fi

# Create host directories for volumes
log "Preparing host directories..."

# Create directories with proper permissions
mkdir -p exports database logs
chmod 755 exports database logs

# Create empty files to ensure proper mounting
touch database/.gitkeep exports/.gitkeep logs/.gitkeep

success "Host directories prepared"

# Show deployment configuration
log "Deployment configuration:"
echo "   • Container Name: $CONTAINER_NAME"
echo "   • Image: $IMAGE_NAME:$IMAGE_TAG"
echo "   • Host Port: $HOST_PORT"
echo "   • Exports Volume: $(pwd)/exports -> /app/exports"
echo "   • Database Volume: $(pwd)/database -> /app/database"
echo "   • Logs Volume: $(pwd)/logs -> /app/logs"
echo ""

# Run the container
log "Starting ADU High-Performance container..."

docker run -d \
    --name "$CONTAINER_NAME" \
    --restart unless-stopped \
    -p "${HOST_PORT}:8501" \
    -v "$(pwd)/exports:/app/exports" \
    -v "$(pwd)/database:/app/database" \
    -v "$(pwd)/logs:/app/logs" \
    --memory="128g" \
    --cpus="16" \
    --shm-size="2g" \
    --ulimit nofile=65536:65536 \
    --ulimit nproc=32768:32768 \
    -e "POLARS_MAX_THREADS=16" \
    -e "POLARS_MAX_MEMORY_USAGE=32GB" \
    -e "CELERY_WORKER_CONCURRENCY=8" \
    "$IMAGE_NAME:$IMAGE_TAG"

container_id=$(docker ps --filter "name=${CONTAINER_NAME}" --format "{{.ID}}")

if [ -n "$container_id" ]; then
    success "Container started successfully!"
    echo ""
    echo "📊 Container Information:"
    echo "========================"
    echo "• Container ID: $container_id"
    echo "• Container Name: $CONTAINER_NAME"
    echo "• Status: $(docker ps --filter "name=${CONTAINER_NAME}" --format "{{.Status}}")"
    echo ""
else
    error "Container failed to start"
fi

# Wait for services to initialize
log "Waiting for services to initialize..."
echo "This may take 30-60 seconds for first startup..."

sleep 10

# Check container health
log "Checking container health..."
container_health="unknown"

for i in {1..12}; do  # Wait up to 2 minutes
    if docker exec "$CONTAINER_NAME" /app/healthcheck.sh &>/dev/null; then
        container_health="healthy"
        break
    fi
    echo -n "."
    sleep 10
done

echo ""

if [ "$container_health" = "healthy" ]; then
    success "All services are healthy and ready!"
else
    warning "Services are still starting up. Check logs if issues persist."
fi

# Show service status
log "Service status:"
docker exec "$CONTAINER_NAME" supervisorctl status 2>/dev/null || echo "   Status check unavailable"

echo ""
echo "🌐 Access Points:"
echo "=================="
echo "• Web Interface: http://localhost:${HOST_PORT}"
echo "• High-Performance UI: http://localhost:${HOST_PORT}/templates/index_realtime.html"
echo "• API Health Check: http://localhost:${HOST_PORT}/api/history"
echo "• Container Health: docker exec ${CONTAINER_NAME} /app/healthcheck.sh"
echo ""

echo "📋 Management Commands:"
echo "======================"
echo "• View logs: docker logs -f ${CONTAINER_NAME}"
echo "• Container shell: docker exec -it ${CONTAINER_NAME} /bin/bash"
echo "• Stop container: docker stop ${CONTAINER_NAME}"
echo "• Restart container: docker restart ${CONTAINER_NAME}"
echo "• Remove container: docker rm -f ${CONTAINER_NAME}"
echo ""

echo "📊 Expected Performance (16-core, 128GB optimal):"
echo "=================================================="
echo "• Throughput: 500K+ rows/second"
echo "• Concurrent Tables: 8 simultaneous"
echo "• Memory Buffer: Up to 32GB"
echo "• Database Connections: 16 concurrent"
echo ""

echo "📁 Data Persistence:"
echo "===================="
echo "• Exported files: ./exports/ directory"
echo "• Job database: ./database/ directory" 
echo "• Application logs: ./logs/ directory"
echo ""

# Show recent logs
log "Recent container logs:"
echo "======================"
docker logs --tail 20 "$CONTAINER_NAME" 2>/dev/null || warning "Could not retrieve logs"

echo ""
success "ADU High-Performance container deployment complete!"
echo ""
echo "🎯 The system is optimized for maximum performance on your 16-core, 128GB air-gapped environment."
echo "   Start your first export job at: http://localhost:${HOST_PORT}"
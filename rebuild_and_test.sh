#!/bin/bash

# Complete rebuild script for ADU Export Application
# Cleans up old images and rebuilds with latest changes

set -e  # Exit on any error

echo "=== ADU Export Application - Complete Rebuild ==="
echo "This script will clean up old Docker artifacts and rebuild the application"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if Docker is running
check_docker() {
    if ! docker info >/dev/null 2>&1; then
        print_error "Docker is not running. Please start Docker and try again."
        exit 1
    fi
    print_success "Docker is running"
}

# Function to stop and remove existing containers
cleanup_containers() {
    print_status "Stopping and removing existing containers..."
    
    # Stop all running containers related to the project
    CONTAINERS=$(docker ps -a -q --filter "name=adu-" 2>/dev/null || true)
    if [ ! -z "$CONTAINERS" ]; then
        print_status "Stopping containers: $CONTAINERS"
        docker stop $CONTAINERS 2>/dev/null || true
        docker rm $CONTAINERS 2>/dev/null || true
        print_success "Removed existing containers"
    else
        print_status "No existing containers to remove"
    fi
}

# Function to remove old images
cleanup_images() {
    print_status "Removing old Docker images..."
    
    # Remove specific images
    IMAGES_TO_REMOVE=(
        "export-app"
        "export-app:latest"
        "export-app:production"
        "adu-export-app"
        "adu-export-app:latest"
    )
    
    for image in "${IMAGES_TO_REMOVE[@]}"; do
        if docker images -q "$image" >/dev/null 2>&1; then
            print_status "Removing image: $image"
            docker rmi "$image" 2>/dev/null || true
        fi
    done
    
    # Remove dangling images
    DANGLING=$(docker images -f "dangling=true" -q 2>/dev/null || true)
    if [ ! -z "$DANGLING" ]; then
        print_status "Removing dangling images..."
        docker rmi $DANGLING 2>/dev/null || true
    fi
    
    print_success "Image cleanup completed"
}

# Function to clean up volumes
cleanup_volumes() {
    print_status "Cleaning up Docker volumes..."
    
    # Remove specific volumes if they exist
    VOLUMES_TO_REMOVE=(
        "adu_redis_data"
        "export_app_redis_data"
    )
    
    for volume in "${VOLUMES_TO_REMOVE[@]}"; do
        if docker volume ls -q | grep -q "^${volume}$" 2>/dev/null; then
            print_status "Removing volume: $volume"
            docker volume rm "$volume" 2>/dev/null || true
        fi
    done
    
    print_success "Volume cleanup completed"
}

# Function to clean up networks
cleanup_networks() {
    print_status "Cleaning up Docker networks..."
    
    # Remove custom networks if they exist
    NETWORKS_TO_REMOVE=(
        "export_app_default"
        "adu_default"
    )
    
    for network in "${NETWORKS_TO_REMOVE[@]}"; do
        if docker network ls | grep -q "$network" 2>/dev/null; then
            print_status "Removing network: $network"
            docker network rm "$network" 2>/dev/null || true
        fi
    done
    
    print_success "Network cleanup completed"
}

# Function to build new images
build_images() {
    print_status "Building new Docker images..."
    
    # Build the main application image
    print_status "Building main application image..."
    docker build -t adu-export-app:latest -f Dockerfile . || {
        print_error "Failed to build main application image"
        exit 1
    }
    
    # Also build the airgapped version
    print_status "Building airgapped image..."
    docker build -t adu-export-app:airgapped -f Dockerfile.airgapped . || {
        print_warning "Failed to build airgapped image (this is optional)"
    }
    
    print_success "Docker images built successfully"
}

# Function to verify the build
verify_build() {
    print_status "Verifying the build..."
    
    # Check if images exist
    if docker images | grep -q "adu-export-app"; then
        print_success "Images created successfully:"
        docker images | grep "adu-export-app" | while read line; do
            echo "  $line"
        done
    else
        print_error "No images found after build"
        exit 1
    fi
    
    # Test a simple container run
    print_status "Testing container startup..."
    CONTAINER_ID=$(docker run -d --name adu-test -p 5001:5000 adu-export-app:latest)
    
    # Wait a moment for startup
    sleep 5
    
    # Check if container is running
    if docker ps | grep -q "adu-test"; then
        print_success "Container started successfully"
        
        # Test if the application responds
        if curl -s http://localhost:5001/health >/dev/null 2>&1; then
            print_success "Application is responding to health check"
        else
            print_warning "Application may not be fully ready yet"
        fi
    else
        print_error "Container failed to start"
        docker logs adu-test
    fi
    
    # Clean up test container
    docker stop adu-test >/dev/null 2>&1 || true
    docker rm adu-test >/dev/null 2>&1 || true
}

# Function to start the application
start_application() {
    print_status "Starting the complete application stack..."
    
    # Use docker-compose to start all services
    if [ -f "docker-compose.celery.yml" ]; then
        print_status "Starting services with Celery (async processing)..."
        docker-compose -f docker-compose.celery.yml up -d
        
        # Wait for services to be ready
        print_status "Waiting for services to be ready..."
        sleep 10
        
        # Check service status
        print_status "Service status:"
        docker-compose -f docker-compose.celery.yml ps
        
        print_success "Application stack started successfully!"
        echo ""
        echo "=== Access Information ==="
        echo "Web Interface: http://localhost:5000"
        echo "Redis: localhost:6379"
        echo ""
        echo "=== Useful Commands ==="
        echo "View logs: docker-compose -f docker-compose.celery.yml logs -f"
        echo "Stop services: docker-compose -f docker-compose.celery.yml down"
        echo "Restart services: docker-compose -f docker-compose.celery.yml restart"
        echo ""
        
    else
        print_warning "docker-compose.celery.yml not found, starting single container..."
        docker run -d --name adu-export-app -p 5000:5000 \
            -v $(pwd)/exports:/app/exports \
            -v $(pwd)/logs:/app/logs \
            adu-export-app:latest
        
        print_success "Single container started on port 5000"
    fi
}

# Function to show helpful information
show_info() {
    echo ""
    echo "=== Application Information ==="
    echo "📊 Multi-Database Export System with Async Processing"
    echo ""
    echo "🔧 Features:"
    echo "  • PostgreSQL, Greenplum, and Vertica support"
    echo "  • Database-driven schema extraction (no inference conflicts)"
    echo "  • Celery async processing with Redis broker"
    echo "  • Parallel chunk processing for large tables"
    echo "  • Real-time job progress tracking"
    echo "  • Production-ready with Gunicorn"
    echo ""
    echo "📁 File Locations:"
    echo "  • Exports: ./exports/"
    echo "  • Logs: ./logs/"
    echo "  • Database: ./adu/data/"
    echo ""
    echo "🧪 Testing:"
    echo "  • Health check: curl http://localhost:5000/health"
    echo "  • API docs: http://localhost:5000/api/docs (if available)"
    echo ""
}

# Main execution
main() {
    echo "Starting complete rebuild process..."
    echo ""
    
    # Parse command line arguments
    SKIP_CLEANUP=false
    START_APP=true
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            --skip-cleanup)
                SKIP_CLEANUP=true
                shift
                ;;
            --build-only)
                START_APP=false
                shift
                ;;
            --help)
                echo "Usage: $0 [OPTIONS]"
                echo ""
                echo "Options:"
                echo "  --skip-cleanup    Skip Docker cleanup (faster rebuild)"
                echo "  --build-only      Only build images, don't start services"
                echo "  --help           Show this help message"
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                echo "Use --help for usage information"
                exit 1
                ;;
        esac
    done
    
    # Execute the rebuild process
    check_docker
    
    if [ "$SKIP_CLEANUP" = false ]; then
        cleanup_containers
        cleanup_images
        cleanup_volumes
        cleanup_networks
        
        # Run docker system prune to clean up everything
        print_status "Running Docker system cleanup..."
        docker system prune -f >/dev/null 2>&1 || true
    else
        print_warning "Skipping cleanup as requested"
        cleanup_containers  # Still stop running containers
    fi
    
    build_images
    verify_build
    
    if [ "$START_APP" = true ]; then
        start_application
        show_info
    else
        print_success "Build completed successfully!"
        echo "Use 'docker-compose -f docker-compose.celery.yml up -d' to start the application"
    fi
    
    echo ""
    print_success "Rebuild process completed successfully! 🎉"
}

# Run the main function
main "$@"

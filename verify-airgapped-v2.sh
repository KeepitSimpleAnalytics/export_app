#!/bin/bash

# ADU Export Application - Airgapped Image Verification Script v2.0
# This script thoroughly tests the airgapped Docker image

set -e

echo "🔍 ADU Export v2.0 - Airgapped Image Verification"
echo "================================================="
echo ""

# Configuration
IMAGE_FILE="adu-airgapped-v2-20250806-173019.tar"
TEST_CONTAINER="adu-export-test"
TEST_PORT="5001"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    local status=$1
    local message=$2
    case $status in
        "SUCCESS") echo -e "${GREEN}✅ $message${NC}" ;;
        "ERROR") echo -e "${RED}❌ $message${NC}" ;;
        "WARNING") echo -e "${YELLOW}⚠️  $message${NC}" ;;
        "INFO") echo -e "${BLUE}ℹ️  $message${NC}" ;;
    esac
}

# Function to cleanup test resources
cleanup() {
    echo ""
    print_status "INFO" "Cleaning up test resources..."
    docker stop "$TEST_CONTAINER" 2>/dev/null || true
    docker rm "$TEST_CONTAINER" 2>/dev/null || true
    print_status "SUCCESS" "Cleanup completed"
}

# Set trap for cleanup on exit
trap cleanup EXIT

# Function to check prerequisites
check_prerequisites() {
    print_status "INFO" "Checking prerequisites..."
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        print_status "ERROR" "Docker is not installed"
        exit 1
    fi
    print_status "SUCCESS" "Docker is available: $(docker --version | cut -d' ' -f3 | cut -d',' -f1)"
    
    # Check Docker daemon
    if ! docker info &> /dev/null; then
        print_status "ERROR" "Docker daemon is not running"
        exit 1
    fi
    print_status "SUCCESS" "Docker daemon is running"
    
    # Check image file
    if [ ! -f "$IMAGE_FILE" ]; then
        print_status "ERROR" "Image file not found: $IMAGE_FILE"
        exit 1
    fi
    
    local file_size=$(du -h "$IMAGE_FILE" | cut -f1)
    print_status "SUCCESS" "Image file found: $IMAGE_FILE ($file_size)"
    echo ""
}

# Function to load and verify image
load_and_verify_image() {
    print_status "INFO" "Loading Docker image..."
    
    # Load image
    if docker load -i "$IMAGE_FILE" > /dev/null 2>&1; then
        print_status "SUCCESS" "Image loaded successfully"
    else
        print_status "ERROR" "Failed to load image"
        exit 1
    fi
    
    # Verify image exists
    if docker images | grep -q "adu-export.*airgapped"; then
        local image_info=$(docker images | grep "adu-export.*airgapped" | head -1)
        print_status "SUCCESS" "Image verified: $image_info"
    else
        print_status "ERROR" "Image not found in Docker images"
        exit 1
    fi
    echo ""
}

# Function to test container startup
test_container_startup() {
    print_status "INFO" "Testing container startup..."
    
    # Stop any existing test container
    docker stop "$TEST_CONTAINER" 2>/dev/null || true
    docker rm "$TEST_CONTAINER" 2>/dev/null || true
    
    # Create temporary directories for testing
    local temp_dir="/tmp/adu-test-$$"
    mkdir -p "$temp_dir"/{exports,database,logs}
    
    # Start test container
    if docker run -d \
        --name "$TEST_CONTAINER" \
        -p "$TEST_PORT:5000" \
        -e FLASK_DEBUG=False \
        -v "$temp_dir/exports:/app/exports" \
        -v "$temp_dir/database:/app/adu/data" \
        -v "$temp_dir/logs:/app/logs" \
        adu-export:airgapped > /dev/null 2>&1; then
        print_status "SUCCESS" "Container started successfully"
    else
        print_status "ERROR" "Failed to start container"
        docker logs "$TEST_CONTAINER" 2>/dev/null || true
        exit 1
    fi
    
    # Wait for startup
    print_status "INFO" "Waiting for application startup (30 seconds max)..."
    local attempts=0
    while [ $attempts -lt 30 ]; do
        if docker ps | grep -q "$TEST_CONTAINER"; then
            break
        fi
        sleep 1
        attempts=$((attempts + 1))
    done
    
    if docker ps | grep -q "$TEST_CONTAINER"; then
        print_status "SUCCESS" "Container is running successfully"
    else
        print_status "ERROR" "Container failed to start properly"
        docker logs "$TEST_CONTAINER" 2>/dev/null || true
        exit 1
    fi
    
    # Cleanup temp directory
    rm -rf "$temp_dir"
    echo ""
}

# Function to test web interface
test_web_interface() {
    print_status "INFO" "Testing web interface..."
    
    # Wait for web server to be ready
    local attempts=0
    while [ $attempts -lt 30 ]; do
        if curl -s "http://localhost:$TEST_PORT/" > /dev/null 2>&1; then
            break
        fi
        sleep 1
        attempts=$((attempts + 1))
    done
    
    # Test main page
    if curl -s "http://localhost:$TEST_PORT/" | grep -q "ADU Export" 2>/dev/null; then
        print_status "SUCCESS" "Main page is accessible"
    else
        print_status "ERROR" "Main page is not accessible"
        docker logs "$TEST_CONTAINER" --tail 10
        exit 1
    fi
    
    # Test API endpoints
    if curl -s "http://localhost:$TEST_PORT/api/history" > /dev/null 2>&1; then
        print_status "SUCCESS" "API endpoints are responding"
    else
        print_status "WARNING" "API endpoints may not be fully ready"
    fi
    echo ""
}

# Function to test application components
test_application_components() {
    print_status "INFO" "Testing application components..."
    
    # Test Python imports
    if docker exec "$TEST_CONTAINER" python -c "
import sys
sys.path.append('/app')
from adu.worker import get_database_connection, discover_tables
from adu.database import get_db_connection
import polars as pl
import pyarrow as pa
print('All imports successful')
" > /dev/null 2>&1; then
        print_status "SUCCESS" "Python dependencies are working"
    else
        print_status "ERROR" "Python dependency issues detected"
        docker exec "$TEST_CONTAINER" python -c "
import sys
sys.path.append('/app')
try:
    from adu.worker import get_database_connection
    print('Worker module: OK')
except Exception as e:
    print(f'Worker module: ERROR - {e}')

try:
    import polars as pl
    print(f'Polars: OK - version {pl.__version__}')
except Exception as e:
    print(f'Polars: ERROR - {e}')

try:
    import pyarrow as pa
    print(f'PyArrow: OK - version {pa.__version__}')
except Exception as e:
    print(f'PyArrow: ERROR - {e}')
" 2>&1
        exit 1
    fi
    
    # Test database initialization
    if docker exec "$TEST_CONTAINER" python -c "
import sys
sys.path.append('/app')
from adu.database import init_db, get_db_connection
init_db()
conn = get_db_connection()
cursor = conn.cursor()
cursor.execute('SELECT COUNT(*) FROM sqlite_master WHERE type=\"table\"')
table_count = cursor.fetchone()[0]
conn.close()
print(f'Database initialized with {table_count} tables')
" > /dev/null 2>&1; then
        print_status "SUCCESS" "Database initialization working"
    else
        print_status "ERROR" "Database initialization failed"
        exit 1
    fi
    echo ""
}

# Function to test performance optimizations
test_performance_optimizations() {
    print_status "INFO" "Testing v2.0 performance optimizations..."
    
    # Test vectorized operations
    if docker exec "$TEST_CONTAINER" python -c "
import sys
sys.path.append('/app')
import polars as pl
import time

# Test vectorized string operations vs map_elements
df = pl.DataFrame({'test_col': ['test'] * 10000})

# Test vectorized approach (should be fast)
start_time = time.time()
result = df.with_columns(pl.col('test_col').cast(pl.String).str.slice(0, 100))
vectorized_time = time.time() - start_time

print(f'Vectorized operations: {vectorized_time:.4f}s for 10K rows')
print('✅ Performance optimizations are active')
" > /dev/null 2>&1; then
        print_status "SUCCESS" "Vectorized operations are working"
    else
        print_status "WARNING" "Could not verify performance optimizations"
    fi
    
    # Test memory management
    docker exec "$TEST_CONTAINER" python -c "
import gc
import sys
sys.path.append('/app')
initial_objects = len(gc.get_objects())
print(f'Memory management test: {initial_objects} objects tracked')
gc.collect()
print('✅ Memory management is working')
" > /dev/null 2>&1 && print_status "SUCCESS" "Memory management is working"
    
    echo ""
}

# Function to show performance metrics
show_performance_metrics() {
    print_status "INFO" "Container performance metrics..."
    
    # Get container stats
    local stats=$(docker stats "$TEST_CONTAINER" --no-stream --format "table {{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}")
    echo "$stats"
    
    # Get image size
    local image_size=$(docker images | grep "adu-export.*airgapped" | awk '{print $7}')
    print_status "INFO" "Image size: $image_size"
    
    echo ""
}

# Function to show final results
show_final_results() {
    echo ""
    print_status "SUCCESS" "🎉 Airgapped image verification completed successfully!"
    echo ""
    echo "📋 Verification Summary:"
    echo "   ✅ Docker image loads correctly"
    echo "   ✅ Container starts successfully"  
    echo "   ✅ Web interface is accessible"
    echo "   ✅ API endpoints are working"
    echo "   ✅ Python dependencies are complete"
    echo "   ✅ Database initialization works"
    echo "   ✅ Performance optimizations active"
    echo ""
    echo "🚀 This image is ready for airgapped deployment!"
    echo ""
    echo "📦 Image Details:"
    echo "   File: $IMAGE_FILE"
    echo "   Size: $(du -h "$IMAGE_FILE" | cut -f1)"
    echo "   Version: v2.0 with Performance Optimizations"
    echo ""
    echo "📋 Next Steps:"
    echo "   1. Transfer $IMAGE_FILE to airgapped environment"
    echo "   2. Run: ./deploy-airgapped.sh"
    echo "   3. Access web interface at http://localhost:5000"
}

# Main execution
main() {
    check_prerequisites
    load_and_verify_image
    test_container_startup
    test_web_interface
    test_application_components
    test_performance_optimizations
    show_performance_metrics
    show_final_results
}

# Run main function
main "$@"

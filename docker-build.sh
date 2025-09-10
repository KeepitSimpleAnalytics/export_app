#!/bin/bash

# ADU Docker Build Script - No Docker Compose Required
# Builds a complete self-contained container with all services

set -e

# Configuration
IMAGE_NAME="adu-high-performance"
IMAGE_TAG="latest"
BUILD_DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ)
BUILD_VERSION="2.0.0"
BUILD_COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo "🐳 ADU High-Performance Docker Build"
echo "===================================="
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

# Pre-build checks
log "Running pre-build checks..."

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    error "Docker is not installed or not in PATH"
fi

# Check Docker daemon
if ! docker info &> /dev/null; then
    error "Docker daemon is not running"
fi

# Check if we're in the right directory
if [ ! -f "adu/app.py" ]; then
    error "Please run this script from the ADU application root directory"
fi

success "Pre-build checks passed"

# Show build information
log "Build configuration:"
echo "   • Image Name: $IMAGE_NAME:$IMAGE_TAG"
echo "   • Build Date: $BUILD_DATE"
echo "   • Version: $BUILD_VERSION"
echo "   • Commit: $BUILD_COMMIT"
echo ""

# Make scripts executable
log "Preparing build context..."
chmod +x docker/*.sh 2>/dev/null || true

# Build the Docker image
log "Starting Docker build..."
echo ""

# Build with progress output
docker build \
    -f Dockerfile.production \
    -t "$IMAGE_NAME:$IMAGE_TAG" \
    --build-arg BUILD_DATE="$BUILD_DATE" \
    --build-arg BUILD_VERSION="$BUILD_VERSION" \
    --build-arg BUILD_COMMIT="$BUILD_COMMIT" \
    --progress=plain \
    .

build_exit_code=$?

if [ $build_exit_code -eq 0 ]; then
    success "Docker build completed successfully!"
else
    error "Docker build failed with exit code $build_exit_code"
fi

# Get image information
log "Analyzing built image..."
image_id=$(docker images --format "{{.ID}}" "$IMAGE_NAME:$IMAGE_TAG" | head -1)
image_size=$(docker images --format "{{.Size}}" "$IMAGE_NAME:$IMAGE_TAG" | head -1)

echo ""
echo "📊 Build Results:"
echo "=================="
echo "• Image ID: $image_id"
echo "• Image Size: $image_size"
echo "• Build Date: $BUILD_DATE"
echo "• Version: $BUILD_VERSION"

# Show next steps
echo ""
echo "🚀 Next Steps:"
echo "=============="
echo ""
echo "1. Run the container:"
echo "   ./docker-run.sh"
echo ""
echo "2. Or manually run with:"
echo "   docker run -d \\"
echo "     --name adu-export \\"
echo "     -p 5000:5000 \\"
echo "     -v \$(pwd)/exports:/app/exports \\"
echo "     -v \$(pwd)/database:/app/database \\"
echo "     -v \$(pwd)/logs:/app/logs \\"
echo "     $IMAGE_NAME:$IMAGE_TAG"
echo ""
echo "3. Access application:"
echo "   http://localhost:5000"
echo ""

success "ADU High-Performance Docker image ready for deployment!"

echo ""
echo "🎯 Performance Specifications:"
echo "• Optimized for 16-core, 128GB systems"
echo "• 8 concurrent table exports"
echo "• 500K+ rows/second throughput"
echo "• Real-time WebSocket progress updates"
echo "• Complete data integrity validation"
echo ""
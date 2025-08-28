#!/bin/bash

# ADU Export Application - Air-gapped Image Builder
# Builds and exports Docker image for air-gapped environments on port 8504

set -e  # Exit on any error

echo "🏗️  ADU Export Application - Air-gapped Image Builder"
echo "==================================================="
echo ""

# Configuration
IMAGE_NAME="adu-export"
IMAGE_TAG="airgapped-8504"
FULL_IMAGE_NAME="${IMAGE_NAME}:${IMAGE_TAG}"
EXPORT_FILE="adu-airgapped-8504-$(date +%Y%m%d-%H%M%S).tar"

echo "📋 Build Configuration:"
echo "   Image Name: $FULL_IMAGE_NAME"
echo "   Export File: $EXPORT_FILE"
echo "   Target Port: 8504"
echo ""

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
    
    # Check if Dockerfile.airgapped exists
    if [ ! -f "Dockerfile.airgapped" ]; then
        echo "❌ Dockerfile.airgapped not found in current directory"
        exit 1
    fi
    echo "✅ Dockerfile.airgapped found"
    
    echo ""
}

# Function to build Docker image
build_image() {
    echo "🏗️  Building Docker image..."
    echo "   This may take several minutes..."
    echo ""
    
    # Build the image with progress output
    docker build \
        -f Dockerfile.airgapped \
        -t "$FULL_IMAGE_NAME" \
        --progress=plain \
        .
    
    if [ $? -eq 0 ]; then
        echo ""
        echo "✅ Image built successfully: $FULL_IMAGE_NAME"
    else
        echo ""
        echo "❌ Image build failed"
        exit 1
    fi
    
    # Show image details
    echo ""
    echo "📦 Image Details:"
    docker images "$FULL_IMAGE_NAME" --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}\t{{.CreatedAt}}"
    echo ""
}

# Function to test the image
test_image() {
    echo "🧪 Testing the image..."
    
    # Remove any existing test container
    docker rm -f adu-test 2>/dev/null || true
    
    # Run a quick test container
    echo "   Starting test container on port 8504..."
    docker run -d \
        --name adu-test \
        -p 8504:8504 \
        "$FULL_IMAGE_NAME"
    
    # Wait for container to start
    echo "   Waiting for application to start..."
    sleep 10
    
    # Test if the application is responding
    if curl -f http://localhost:8504/ > /dev/null 2>&1; then
        echo "✅ Application is responding on port 8504"
        TEST_SUCCESS=true
    else
        echo "⚠️  Application may not be responding properly"
        echo "   Container logs:"
        docker logs adu-test --tail 20
        TEST_SUCCESS=false
    fi
    
    # Clean up test container
    docker stop adu-test > /dev/null 2>&1
    docker rm adu-test > /dev/null 2>&1
    
    if [ "$TEST_SUCCESS" = false ]; then
        echo "❌ Image test failed"
        exit 1
    fi
    
    echo ""
}

# Function to export image
export_image() {
    echo "📦 Exporting image for air-gapped deployment..."
    
    # Export the image
    docker save "$FULL_IMAGE_NAME" -o "$EXPORT_FILE"
    
    if [ $? -eq 0 ]; then
        echo "✅ Image exported successfully: $EXPORT_FILE"
        
        # Show file details
        FILE_SIZE=$(du -h "$EXPORT_FILE" | cut -f1)
        echo "   File size: $FILE_SIZE"
        echo ""
    else
        echo "❌ Image export failed"
        exit 1
    fi
}

# Function to create deployment instructions
create_instructions() {
    INSTRUCTIONS_FILE="DEPLOYMENT_INSTRUCTIONS_8504.md"
    
    cat > "$INSTRUCTIONS_FILE" << EOF
# ADU Export Application - Air-gapped Deployment Instructions (Port 8504)

## Generated: $(date)
## Image File: $EXPORT_FILE

### Prerequisites
- Docker installed and running
- At least 8GB RAM (16GB+ recommended)
- At least 4 CPU cores (8+ recommended)
- Root/sudo access for directory creation

### Deployment Steps

1. **Transfer the image file to your air-gapped environment:**
   \`\`\`bash
   # Copy $EXPORT_FILE to your target server
   scp $EXPORT_FILE user@target-server:/path/to/deployment/
   \`\`\`

2. **Load the Docker image:**
   \`\`\`bash
   docker load -i $EXPORT_FILE
   \`\`\`

3. **Create data directories:**
   \`\`\`bash
   sudo mkdir -p /data/adu-export/{exports,database,logs,temp}
   sudo chmod 755 /data/adu-export/{exports,database,logs,temp}
   \`\`\`

4. **Run the container:**
   \`\`\`bash
   docker run -d \\
     --name adu-export-prod \\
     --restart unless-stopped \\
     -p 8504:8504 \\
     -v /data/adu-export/exports:/app/exports \\
     -v /data/adu-export/logs:/app/logs \\
     -v /data/adu-export/database:/tmp/adu \\
     --memory="16g" \\
     --cpus="8" \\
     $FULL_IMAGE_NAME
   \`\`\`

5. **Verify deployment:**
   \`\`\`bash
   # Check container status
   docker ps | grep adu-export-prod
   
   # Check logs
   docker logs adu-export-prod
   
   # Test web interface
   curl http://localhost:8504/
   \`\`\`

### Access
- **Web Interface:** http://your-server-ip:8504
- **Container Logs:** \`docker logs adu-export-prod\`
- **Export Files:** \`/data/adu-export/exports/\`

### Management Commands

**Start container:**
\`\`\`bash
docker start adu-export-prod
\`\`\`

**Stop container:**
\`\`\`bash
docker stop adu-export-prod
\`\`\`

**Restart container:**
\`\`\`bash
docker restart adu-export-prod
\`\`\`

**View logs:**
\`\`\`bash
docker logs -f adu-export-prod
\`\`\`

**Update container (with new image):**
\`\`\`bash
docker stop adu-export-prod
docker rm adu-export-prod
# Load new image and run step 4 again
\`\`\`

### Troubleshooting

**Container won't start:**
- Check Docker daemon is running
- Verify port 8504 is not already in use
- Check container logs: \`docker logs adu-export-prod\`

**Can't access web interface:**
- Verify container is running: \`docker ps\`
- Check firewall allows port 8504
- Test locally: \`curl http://localhost:8504/\`

**Performance issues:**
- Increase memory/CPU limits in run command
- Monitor system resources: \`htop\` or \`docker stats\`

### Security Notes
- This is designed for air-gapped environments
- No external network access required
- All data stored locally in mounted volumes
- Consider additional firewall rules as needed

EOF

    echo "📋 Created deployment instructions: $INSTRUCTIONS_FILE"
    echo ""
}

# Function to show completion summary
show_summary() {
    echo "🎉 Air-gapped image build completed successfully!"
    echo ""
    echo "📁 Generated Files:"
    echo "   - Docker image file: $EXPORT_FILE ($(du -h "$EXPORT_FILE" | cut -f1))"
    echo "   - Deployment instructions: DEPLOYMENT_INSTRUCTIONS_8504.md"
    echo ""
    echo "🚀 Next Steps:"
    echo "   1. Transfer $EXPORT_FILE to your air-gapped environment"
    echo "   2. Follow the instructions in DEPLOYMENT_INSTRUCTIONS_8504.md"
    echo "   3. Load and run the image on port 8504"
    echo ""
    echo "💡 Quick deployment command:"
    echo "   docker load -i $EXPORT_FILE"
    echo "   docker run -d --name adu-export-prod -p 8504:8504 $FULL_IMAGE_NAME"
    echo ""
}

# Main execution
main() {
    check_prerequisites
    build_image
    test_image
    export_image
    create_instructions
    show_summary
}

# Run main function
main "$@"

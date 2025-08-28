#!/bin/bash

# ADU Export Application - Complete Air-Gapped Package Builder
# Creates everything needed for air-gapped deployment without Docker Compose

set -e

echo "🚀 ADU Export Application - Air-Gapped Package Builder"
echo "===================================================="
echo ""

# Configuration
IMAGE_NAME="adu-export"
IMAGE_TAG="airgapped-$(date +%Y%m%d-%H%M%S)"
FULL_IMAGE_NAME="${IMAGE_NAME}:${IMAGE_TAG}"
PACKAGE_NAME="adu-airgapped-complete-$(date +%Y%m%d-%H%M%S)"
PACKAGE_DIR="${PACKAGE_NAME}"

echo "📋 Build Configuration:"
echo "   Image Name: $FULL_IMAGE_NAME"
echo "   Package Name: $PACKAGE_NAME"
echo "   Build Time: $(date)"
echo ""

# Clean up any existing package directory
if [ -d "$PACKAGE_DIR" ]; then
    echo "🧹 Cleaning up existing package directory..."
    rm -rf "$PACKAGE_DIR"
fi

# Create package structure
echo "📁 Creating package structure..."
mkdir -p "$PACKAGE_DIR"/{scripts,docs,runtime}

# Build the Docker image
echo "🏗️  Building Docker image..."
echo "   This may take several minutes..."
echo ""

docker build \
    -f Dockerfile.airgapped \
    -t "$FULL_IMAGE_NAME" \
    --progress=plain \
    .

if [ $? -ne 0 ]; then
    echo "❌ Image build failed"
    exit 1
fi

echo ""
echo "✅ Image built successfully: $FULL_IMAGE_NAME"

# Export the Docker image
echo "📦 Exporting Docker image..."
EXPORT_FILE="${PACKAGE_DIR}/${IMAGE_NAME}-image.tar"
docker save "$FULL_IMAGE_NAME" -o "$EXPORT_FILE"

if [ $? -eq 0 ]; then
    echo "✅ Image exported to: $EXPORT_FILE"
    IMAGE_SIZE=$(du -h "$EXPORT_FILE" | cut -f1)
    echo "   Image size: $IMAGE_SIZE"
else
    echo "❌ Image export failed"
    exit 1
fi

# Create load script
echo "📝 Creating load script..."
cat > "${PACKAGE_DIR}/scripts/load-image.sh" << 'EOF'
#!/bin/bash

# Load ADU Export Docker Image
# Run this script to load the Docker image in your air-gapped environment

set -e

echo "🚀 Loading ADU Export Docker Image"
echo "================================="
echo ""

# Check if Docker is installed and running
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

if ! docker info &> /dev/null; then
    echo "❌ Docker daemon is not running. Please start Docker service."
    exit 1
fi

# Find the image file
IMAGE_FILE=""
for file in ../adu-export-image.tar adu-export-image.tar; do
    if [ -f "$file" ]; then
        IMAGE_FILE="$file"
        break
    fi
done

if [ -z "$IMAGE_FILE" ]; then
    echo "❌ Image file not found. Expected: adu-export-image.tar"
    exit 1
fi

echo "📦 Loading image from: $IMAGE_FILE"
IMAGE_SIZE=$(du -h "$IMAGE_FILE" | cut -f1)
echo "   Image size: $IMAGE_SIZE"
echo ""

# Load the image
docker load -i "$IMAGE_FILE"

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Image loaded successfully!"
    echo ""
    echo "📋 Available ADU images:"
    docker images | grep adu-export
    echo ""
    echo "🎯 Next step: Run './run-container.sh' to start the application"
else
    echo "❌ Failed to load image"
    exit 1
fi
EOF

# Create run script (without Docker Compose)
echo "📝 Creating run script..."
cat > "${PACKAGE_DIR}/scripts/run-container.sh" << 'EOF'
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
EOF

# Create stop script
echo "📝 Creating stop script..."
cat > "${PACKAGE_DIR}/scripts/stop-container.sh" << 'EOF'
#!/bin/bash

# Stop ADU Export Application Container

set -e

echo "🛑 Stopping ADU Export Application"
echo "================================="
echo ""

CONTAINER_NAME="adu-export-app"

# Check if container exists and is running
if docker ps --format "{{.Names}}" | grep -q "^${CONTAINER_NAME}$"; then
    echo "🛑 Stopping container: $CONTAINER_NAME"
    docker stop "$CONTAINER_NAME"
    echo "✅ Container stopped"
elif docker ps -a --format "{{.Names}}" | grep -q "^${CONTAINER_NAME}$"; then
    echo "ℹ️  Container $CONTAINER_NAME is already stopped"
else
    echo "ℹ️  Container $CONTAINER_NAME not found"
fi

echo ""
echo "🔧 To restart: ./run-container.sh"
echo "🗑️  To remove: docker rm $CONTAINER_NAME"
EOF

# Create status script
echo "📝 Creating status script..."
cat > "${PACKAGE_DIR}/scripts/status.sh" << 'EOF'
#!/bin/bash

# Check ADU Export Application Status

echo "📊 ADU Export Application Status"
echo "==============================="
echo ""

CONTAINER_NAME="adu-export-app"

# Check Docker
if ! command -v docker &> /dev/null; then
    echo "❌ Docker not installed"
    exit 1
fi

if ! docker info &> /dev/null; then
    echo "❌ Docker daemon not running"
    exit 1
fi

echo "✅ Docker is running"

# Check images
echo ""
echo "📦 ADU Images:"
docker images | grep adu-export || echo "❌ No ADU images found"

# Check container status
echo ""
echo "🐳 Container Status:"
if docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep "$CONTAINER_NAME"; then
    echo ""
    echo "✅ Container is running"
    
    # Check application health
    echo ""
    echo "🔍 Application Health Check:"
    if curl -f http://localhost:8080/ >/dev/null 2>&1; then
        echo "✅ Application responding on http://localhost:8080"
    else
        echo "❌ Application not responding"
        echo ""
        echo "📋 Recent logs:"
        docker logs --tail 10 "$CONTAINER_NAME"
    fi
elif docker ps -a --format "{{.Names}}" | grep -q "$CONTAINER_NAME"; then
    echo "⚠️  Container exists but is not running"
    echo ""
    echo "📋 Container details:"
    docker ps -a | grep "$CONTAINER_NAME"
    echo ""
    echo "🔧 To start: ./run-container.sh"
else
    echo "❌ Container not found"
    echo ""
    echo "🔧 To create: ./run-container.sh"
fi

# Show disk usage
echo ""
echo "💾 Disk Usage:"
if [ -d "../runtime/exports" ]; then
    EXPORTS_SIZE=$(du -sh ../runtime/exports 2>/dev/null | cut -f1 || echo "0")
    echo "   Exports: $EXPORTS_SIZE"
fi
if [ -d "../runtime/data" ]; then
    DATA_SIZE=$(du -sh ../runtime/data 2>/dev/null | cut -f1 || echo "0")
    echo "   Data: $DATA_SIZE"
fi
EOF

# Create cleanup script
echo "📝 Creating cleanup script..."
cat > "${PACKAGE_DIR}/scripts/cleanup.sh" << 'EOF'
#!/bin/bash

# Cleanup ADU Export Application

echo "🧹 ADU Export Application Cleanup"
echo "================================="
echo ""

CONTAINER_NAME="adu-export-app"

# Stop and remove container
if docker ps -a --format "{{.Names}}" | grep -q "^${CONTAINER_NAME}$"; then
    echo "🛑 Stopping and removing container..."
    docker stop "$CONTAINER_NAME" >/dev/null 2>&1 || true
    docker rm "$CONTAINER_NAME" >/dev/null 2>&1 || true
    echo "✅ Container removed"
else
    echo "ℹ️  No container to remove"
fi

# Optionally remove images
echo ""
read -p "🗑️  Remove ADU Docker images? (y/N): " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🗑️  Removing ADU images..."
    docker images --format "{{.Repository}}:{{.Tag}}" | grep adu-export | xargs -r docker rmi
    echo "✅ Images removed"
fi

# Optionally remove data
echo ""
read -p "🗑️  Remove exported data and database? (y/N): " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🗑️  Removing data directories..."
    rm -rf ../runtime/exports ../runtime/data
    echo "✅ Data removed"
fi

echo ""
echo "✅ Cleanup complete"
EOF

# Make scripts executable
chmod +x "${PACKAGE_DIR}/scripts/"*.sh

# Create comprehensive documentation
echo "📝 Creating documentation..."
cat > "${PACKAGE_DIR}/docs/README.md" << 'EOF'
# ADU Export Application - Air-Gapped Deployment

This package contains everything needed to deploy the ADU Export Application in an air-gapped environment without Docker Compose.

## Package Contents

```
adu-airgapped-complete-[timestamp]/
├── adu-export-image.tar          # Docker image file
├── scripts/                      # Management scripts
│   ├── load-image.sh            # Load Docker image
│   ├── run-container.sh         # Start application
│   ├── stop-container.sh        # Stop application
│   ├── status.sh                # Check status
│   └── cleanup.sh               # Remove everything
├── docs/                        # Documentation
│   ├── README.md                # This file
│   └── TROUBLESHOOTING.md       # Common issues
└── runtime/                     # Created automatically
    ├── exports/                 # Exported data
    └── data/                    # Application database
```

## Prerequisites

- Linux system with Docker installed and running
- At least 2GB free disk space
- Network access not required after installation

## Quick Start

1. **Transfer this entire package** to your air-gapped system
2. **Navigate to the scripts directory:**
   ```bash
   cd adu-airgapped-complete-[timestamp]/scripts
   ```
3. **Load the Docker image:**
   ```bash
   ./load-image.sh
   ```
4. **Start the application:**
   ```bash
   ./run-container.sh
   ```
5. **Access the web interface:**
   - Open browser to: http://localhost:8080

## Management Commands

### Start Application
```bash
./run-container.sh
```

### Check Status
```bash
./status.sh
```

### Stop Application
```bash
./stop-container.sh
```

### View Logs
```bash
docker logs adu-export-app
```

### Complete Cleanup
```bash
./cleanup.sh
```

## Application Features

- **Web Interface:** http://localhost:8080
- **Database Export:** Supports PostgreSQL, Greenplum, Vertica
- **Large Table Support:** Automatic chunking and parallel processing
- **Job Management:** Track export progress and history
- **Data Storage:** Exports saved to `../runtime/exports/`

## Configuration

The application runs with these defaults:
- **Port:** 8080
- **Container Name:** adu-export-app
- **Exports Directory:** `../runtime/exports/`
- **Database Directory:** `../runtime/data/`

### Custom Port
To use a different port, edit `run-container.sh` and change:
```bash
HOST_PORT="8080"  # Change to your preferred port
```

### Custom Directories
To use different storage locations, edit `run-container.sh` and change:
```bash
HOST_EXPORTS_DIR="/your/custom/exports/path"
HOST_DATA_DIR="/your/custom/data/path"
```

## Security Notes

- Application is designed for air-gapped environments
- No external network access required
- Database credentials are not stored permanently
- All data remains on your local system

## System Requirements

- **CPU:** 2+ cores recommended
- **Memory:** 4GB+ RAM recommended
- **Storage:** 
  - 1GB for application
  - Additional space for exported data
- **Docker:** Version 20.10+ recommended

## Support

This is a standalone deployment package. Check the troubleshooting guide for common issues.
EOF

# Create troubleshooting guide
cat > "${PACKAGE_DIR}/docs/TROUBLESHOOTING.md" << 'EOF'
# Troubleshooting Guide

## Common Issues

### Container Won't Start

**Symptoms:** `run-container.sh` fails or container stops immediately

**Solutions:**
1. Check if port 8080 is available:
   ```bash
   netstat -tlnp | grep 8080
   ```
2. Try a different port in `run-container.sh`
3. Check Docker logs:
   ```bash
   docker logs adu-export-app
   ```
4. Ensure directories are writable:
   ```bash
   ls -la ../runtime/
   ```

### Application Not Responding

**Symptoms:** Browser shows "connection refused" or timeouts

**Solutions:**
1. Check container status:
   ```bash
   ./status.sh
   ```
2. Verify port mapping:
   ```bash
   docker port adu-export-app
   ```
3. Check application logs:
   ```bash
   docker logs -f adu-export-app
   ```

### Database Connection Issues

**Symptoms:** "Connection failed" errors in web interface

**Solutions:**
1. Verify database server is accessible from container
2. Check firewall rules
3. Ensure database credentials are correct
4. For PostgreSQL/Greenplum: verify `pg_hba.conf` allows connections

### Export Failures

**Symptoms:** Jobs fail or hang during export

**Solutions:**
1. Check available disk space:
   ```bash
   df -h
   ```
2. Monitor memory usage:
   ```bash
   docker stats adu-export-app
   ```
3. Check worker logs in web interface: http://localhost:8080/logs
4. Reduce chunk size for large tables

### Permission Issues

**Symptoms:** "Permission denied" errors

**Solutions:**
1. Ensure Docker daemon is running:
   ```bash
   sudo systemctl status docker
   ```
2. Add user to docker group:
   ```bash
   sudo usermod -aG docker $USER
   ```
3. Check directory permissions:
   ```bash
   ls -la ../runtime/
   ```

## Performance Tuning

### For Large Databases
- Increase container memory limit
- Adjust chunk size in web interface
- Use fewer parallel workers for memory-constrained systems

### For Better Performance
- Use SSD storage for exports directory
- Ensure database server has adequate resources
- Monitor network bandwidth for remote databases

## Logs and Debugging

### Application Logs
```bash
docker logs adu-export-app
```

### Real-time Logs
```bash
docker logs -f adu-export-app
```

### Container Shell Access
```bash
docker exec -it adu-export-app /bin/bash
```

### Check Container Resources
```bash
docker stats adu-export-app
```

## Recovery Procedures

### Reset Application
```bash
./stop-container.sh
./cleanup.sh
./load-image.sh
./run-container.sh
```

### Backup Exports
```bash
tar -czf exports-backup-$(date +%Y%m%d).tar.gz ../runtime/exports/
```

### Restore Exports
```bash
tar -xzf exports-backup-[date].tar.gz -C ../runtime/
```

## Getting Help

1. Check container status: `./status.sh`
2. Review logs: `docker logs adu-export-app`
3. Verify system resources: `df -h && free -h`
4. Check network connectivity to database
EOF

# Create version info
echo "📝 Creating version info..."
cat > "${PACKAGE_DIR}/VERSION" << EOF
ADU Export Application - Air-Gapped Package
==========================================

Package Created: $(date)
Docker Image: $FULL_IMAGE_NAME
Package Version: $PACKAGE_NAME

Components:
- Application: ADU Export Tool
- Database Support: PostgreSQL, Greenplum, Vertica
- Export Format: Parquet files
- Web Interface: Flask-based dashboard
- Task Queue: Celery with Redis
- Parallel Processing: Multi-threaded exports

Build Environment:
- Host: $(hostname)
- User: $(whoami)
- Docker Version: $(docker --version)
- Build Date: $(date)

Package Size: $(du -sh "$PACKAGE_DIR" | cut -f1)
EOF

# Create quick start script
echo "📝 Creating quick start script..."
cat > "${PACKAGE_DIR}/QUICK_START.sh" << 'EOF'
#!/bin/bash

echo "🚀 ADU Export Application - Quick Start"
echo "======================================"
echo ""
echo "This will load and start the ADU Export Application"
echo ""

# Navigate to scripts directory
cd scripts

# Load image
echo "1️⃣  Loading Docker image..."
./load-image.sh

if [ $? -eq 0 ]; then
    echo ""
    echo "2️⃣  Starting application..."
    ./run-container.sh
    
    if [ $? -eq 0 ]; then
        echo ""
        echo "🎉 SUCCESS! Application is running"
        echo ""
        echo "🌐 Open your browser to: http://localhost:8080"
        echo "📖 Read the documentation in: docs/README.md"
        echo "🔧 Use './scripts/status.sh' to check application status"
    fi
else
    echo "❌ Failed to load image. Check docs/TROUBLESHOOTING.md"
fi
EOF

chmod +x "${PACKAGE_DIR}/QUICK_START.sh"

# Create package archive
echo "📦 Creating final package archive..."
tar -czf "${PACKAGE_NAME}.tar.gz" "$PACKAGE_DIR"

if [ $? -eq 0 ]; then
    ARCHIVE_SIZE=$(du -h "${PACKAGE_NAME}.tar.gz" | cut -f1)
    echo "✅ Package created: ${PACKAGE_NAME}.tar.gz"
    echo "   Archive size: $ARCHIVE_SIZE"
    
    # Clean up directory (keep only the archive)
    rm -rf "$PACKAGE_DIR"
    
    echo ""
    echo "🎉 Air-gapped package ready!"
    echo ""
    echo "📋 Package Details:"
    echo "   File: ${PACKAGE_NAME}.tar.gz"
    echo "   Size: $ARCHIVE_SIZE"
    echo "   Contains: Docker image + all scripts + documentation"
    echo ""
    echo "📤 Transfer Instructions:"
    echo "   1. Copy ${PACKAGE_NAME}.tar.gz to your air-gapped system"
    echo "   2. Extract: tar -xzf ${PACKAGE_NAME}.tar.gz"
    echo "   3. Run: cd ${PACKAGE_NAME} && ./QUICK_START.sh"
    echo ""
    echo "📖 Full documentation included in package"
else
    echo "❌ Failed to create package archive"
    exit 1
fi
EOF

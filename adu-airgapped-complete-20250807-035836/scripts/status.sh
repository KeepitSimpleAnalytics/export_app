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

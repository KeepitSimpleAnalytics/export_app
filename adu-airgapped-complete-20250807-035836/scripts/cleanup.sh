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

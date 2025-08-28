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

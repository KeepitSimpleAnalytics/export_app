#!/bin/bash

# Start ADU Export Application with Celery Workers
# This script builds and starts the complete async processing system

set -e

echo "🚀 Starting ADU Export Application with Async Processing"
echo "=================================================="

# Build the application image
echo "📦 Building application image..."
docker build -t adu-export:celery -f Dockerfile .

# Stop any existing containers
echo "🛑 Stopping existing containers..."
docker-compose -f docker-compose.celery.yml down --remove-orphans

# Start the services
echo "▶️  Starting services (Redis, Web, Workers)..."
docker-compose -f docker-compose.celery.yml up -d

# Wait for services to be ready
echo "⏳ Waiting for services to start..."
sleep 10

# Check service status
echo "📊 Service Status:"
docker-compose -f docker-compose.celery.yml ps

echo ""
echo "🎉 ADU Export Application with Celery is running!"
echo ""
echo "📝 Service URLs:"
echo "   - Web Application: http://localhost:5000"
echo "   - Redis (internal): redis://localhost:6379"
echo ""
echo "🔧 Available Commands:"
echo "   - View logs: docker-compose -f docker-compose.celery.yml logs -f [service]"
echo "   - Stop all: docker-compose -f docker-compose.celery.yml down"
echo "   - Restart: docker-compose -f docker-compose.celery.yml restart [service]"
echo ""
echo "📈 Monitor Workers:"
echo "   - Worker 1: docker logs -f adu-worker"
echo "   - Worker 2: docker logs -f adu-worker2"
echo ""
echo "🏗️  System Architecture:"
echo "   - Web Server: Gunicorn with Flask app"
echo "   - Task Queue: Celery with Redis broker"
echo "   - Workers: 2 async job processors"
echo "   - Database: SQLite with Celery task tracking"

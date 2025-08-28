#!/bin/bash

# Celery Worker Startup Script
# This script starts the Celery worker for processing async jobs

set -e

echo "Starting Celery worker for asynchronous job processing..."

# Set Python path
export PYTHONPATH=/app:$PYTHONPATH

# Navigate to app directory
cd /app

# Set up logging directory
mkdir -p /tmp/celery
mkdir -p /app/logs

# Start Celery worker with optimized configuration
exec celery -A adu.celery_config.celery_app worker \
    --loglevel=info \
    --logfile=/tmp/celery/worker.log \
    --pidfile=/tmp/celery/worker.pid \
    --concurrency=2 \
    --prefetch-multiplier=1 \
    --max-tasks-per-child=1000 \
    --without-heartbeat \
    --without-mingle \
    --without-gossip \
    --queues=export_jobs \
    --hostname=worker@%h

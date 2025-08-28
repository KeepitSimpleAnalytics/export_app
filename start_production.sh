#!/bin/bash

# Production startup script using Gunicorn
# This script replaces the development Flask server with production-ready Gunicorn

set -e  # Exit on any error

echo "=== ADU Export Application - Production Startup ==="

# Set Python path to include current directory for imports
export PYTHONPATH=/app:$PYTHONPATH

# Define the path for the database
DB_FILE="/tmp/adu.db"

# Load environment variables if .env file exists
if [ -f "/app/.env" ]; then
    echo "Loading environment variables from .env file..."
    set -a  # automatically export all variables
    source /app/.env
    set +a
fi

# Set default values for Gunicorn if not provided
export PORT=${PORT:-5000}
export GUNICORN_WORKERS=${GUNICORN_WORKERS:-4}
export GUNICORN_ACCESS_LOG=${GUNICORN_ACCESS_LOG:-/tmp/gunicorn_access.log}
export GUNICORN_ERROR_LOG=${GUNICORN_ERROR_LOG:-/tmp/gunicorn_error.log}
export GUNICORN_LOG_LEVEL=${GUNICORN_LOG_LEVEL:-info}

echo "Configuration:"
echo "  - Port: $PORT"
echo "  - Workers: $GUNICORN_WORKERS"
echo "  - Access Log: $GUNICORN_ACCESS_LOG"
echo "  - Error Log: $GUNICORN_ERROR_LOG"
echo "  - Log Level: $GUNICORN_LOG_LEVEL"

# Check if the database file already exists.
# If it doesn't, run the initialization script.
if [ ! -f "$DB_FILE" ]; then
    echo "Database not found. Initializing..."
    python3 init_database.py
else
    echo "Database already exists. Skipping initialization."
fi

# Create log directories if they don't exist
mkdir -p "$(dirname "$GUNICORN_ACCESS_LOG")" "$(dirname "$GUNICORN_ERROR_LOG")"

# Function to handle shutdown gracefully
shutdown() {
    echo "Shutting down ADU Export Application..."
    if [ ! -z "$GUNICORN_PID" ]; then
        echo "Stopping Gunicorn (PID: $GUNICORN_PID)..."
        kill -TERM "$GUNICORN_PID" 2>/dev/null || true
        wait "$GUNICORN_PID" 2>/dev/null || true
    fi
    if [ ! -z "$WORKER_PID" ]; then
        echo "Stopping Worker (PID: $WORKER_PID)..."
        kill -TERM "$WORKER_PID" 2>/dev/null || true
        wait "$WORKER_PID" 2>/dev/null || true
    fi
    echo "Shutdown complete."
    exit 0
}

# Set up signal handlers
trap shutdown SIGTERM SIGINT

echo "Starting Gunicorn server..."
# Start Gunicorn with the Flask app in the background
gunicorn \
    --config gunicorn.conf.py \
    --bind "0.0.0.0:$PORT" \
    --workers "$GUNICORN_WORKERS" \
    --access-logfile "$GUNICORN_ACCESS_LOG" \
    --error-logfile "$GUNICORN_ERROR_LOG" \
    --log-level "$GUNICORN_LOG_LEVEL" \
    --pid /tmp/gunicorn.pid \
    "adu.app:app" &

GUNICORN_PID=$!
echo "Gunicorn started with PID: $GUNICORN_PID"

# Give Gunicorn a moment to start
sleep 2

# Check if Gunicorn is running
if ! kill -0 "$GUNICORN_PID" 2>/dev/null; then
    echo "ERROR: Gunicorn failed to start!"
    exit 1
fi

echo "Starting worker process..."
# Start the worker process in the background
python3 adu/worker.py &
WORKER_PID=$!
echo "Worker started with PID: $WORKER_PID"

echo "=== ADU Export Application Started Successfully ==="
echo "  - Gunicorn PID: $GUNICORN_PID"
echo "  - Worker PID: $WORKER_PID"
echo "  - Web interface: http://0.0.0.0:$PORT"

# Monitor both processes
while true; do
    # Check if Gunicorn is still running
    if ! kill -0 "$GUNICORN_PID" 2>/dev/null; then
        echo "ERROR: Gunicorn process died! Shutting down..."
        shutdown
    fi
    
    # Check if Worker is still running
    if ! kill -0 "$WORKER_PID" 2>/dev/null; then
        echo "ERROR: Worker process died! Shutting down..."
        shutdown
    fi
    
    # Wait a bit before checking again
    sleep 10
done

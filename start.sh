#!/bin/bash

# Set Python path to include current directory for imports
export PYTHONPATH=/app:$PYTHONPATH

# Start Redis server in background
echo "Starting Redis server..."
redis-server --daemonize yes --port 6379 --bind 127.0.0.1 --save 60 1 --loglevel notice

# Wait for Redis to be ready
echo "Waiting for Redis to start..."
sleep 3

# Verify Redis is running
redis-cli ping > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "✅ Redis is running and responsive"
else
    echo "❌ Redis failed to start properly"
    exit 1
fi

# Set Redis environment variables for Celery
export CELERY_BROKER_URL="redis://localhost:6379/0"
export CELERY_RESULT_BACKEND="redis://localhost:6379/0"

# Define the path for the database
DB_FILE="/tmp/adu.db"

# Check if the database file already exists.
# If it doesn't, run the initialization script.
if [ ! -f "$DB_FILE" ]; then
    echo "Database not found. Initializing..."
    python3 init_database.py
else
    echo "Database already exists. Skipping initialization."
fi

# Start the Flask server in the background
echo "Starting Flask server..."
flask run &

# Start the worker process in the foreground
echo "Starting worker..."
python3 adu/worker.py
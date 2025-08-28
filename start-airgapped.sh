#!/bin/bash

# Air-gapped ADU Export Application Startup Script
# Optimized for port 8504 deployment

set -e  # Exit on any error

echo "🚀 Starting ADU Export Application (Air-gapped Mode)"
echo "=================================================="
echo "Port: 8504"
echo "Database: ${ADU_DB_PATH:-/tmp/adu/adu.db}"
echo "Output: ${ADU_OUTPUT_PATH:-/app/exports}"
echo "Logs: ${ADU_LOG_PATH:-/app/logs}"
echo ""

# Ensure directories exist
mkdir -p "$(dirname "${ADU_DB_PATH:-/tmp/adu/adu.db}")"
mkdir -p "${ADU_OUTPUT_PATH:-/app/exports}"
mkdir -p "${ADU_LOG_PATH:-/app/logs}"

# Define the path for the database
DB_FILE="${ADU_DB_PATH:-/tmp/adu/adu.db}"

# Check if the database file already exists.
# If it doesn't, run the initialization script.
if [ ! -f "$DB_FILE" ]; then
    echo "📊 Database not found. Initializing..."
    python3 init_database.py
    echo "✅ Database initialized successfully."
else
    echo "✅ Database already exists. Skipping initialization."
fi

# Set Flask configuration for port 8504
export FLASK_RUN_PORT=8504
export FLASK_RUN_HOST=0.0.0.0
export FLASK_ENV=production

echo ""
echo "🌐 Starting Flask server on port 8504..."
# Start the Flask server in the background
python3 -m flask run --host=0.0.0.0 --port=8504 &
FLASK_PID=$!

# Wait a moment for Flask to start
sleep 3

# Check if Flask started successfully
if ! curl -f http://localhost:8504/ > /dev/null 2>&1; then
    echo "⚠️  Flask server may not have started properly"
    echo "   Continuing with worker startup..."
else
    echo "✅ Flask server started successfully"
fi

echo ""
echo "⚙️  Starting worker process..."
# Start the worker process in the foreground
python3 adu/worker.py &
WORKER_PID=$!

echo ""
echo "🎯 ADU Export Application is running:"
echo "   Web Interface: http://0.0.0.0:8504"
echo "   Flask PID: $FLASK_PID"
echo "   Worker PID: $WORKER_PID"
echo ""
echo "📝 Logs will be written to: ${ADU_LOG_PATH:-/app/logs}"
echo "📁 Exports will be saved to: ${ADU_OUTPUT_PATH:-/app/exports}"
echo ""

# Function to handle shutdown gracefully
shutdown() {
    echo ""
    echo "🛑 Shutting down gracefully..."
    
    if [ ! -z "$WORKER_PID" ]; then
        echo "   Stopping worker process (PID: $WORKER_PID)..."
        kill -TERM "$WORKER_PID" 2>/dev/null || true
    fi
    
    if [ ! -z "$FLASK_PID" ]; then
        echo "   Stopping Flask server (PID: $FLASK_PID)..."
        kill -TERM "$FLASK_PID" 2>/dev/null || true
    fi
    
    echo "✅ Shutdown complete"
    exit 0
}

# Trap signals for graceful shutdown
trap shutdown SIGTERM SIGINT

# Wait for either process to exit
wait

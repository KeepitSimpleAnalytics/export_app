#!/bin/bash
# Production startup script for Air-gapped Data Utility (ADU)

set -e

# Configuration
APP_DIR="/opt/adu"
VENV_DIR="$APP_DIR/venv"
LOG_DIR="/var/log/adu"
DATA_DIR="/opt/adu/data"
EXPORTS_DIR="/opt/adu/exports"
USER="adu"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

echo_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

echo_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if running as root for initial setup
if [[ $EUID -eq 0 ]]; then
    echo_info "Running initial setup as root..."
    
    # Create user if it doesn't exist
    if ! id "$USER" &>/dev/null; then
        echo_info "Creating user: $USER"
        useradd -r -s /bin/bash -d $APP_DIR $USER
    fi
    
    # Create directories
    echo_info "Creating directories..."
    mkdir -p $APP_DIR $LOG_DIR $DATA_DIR $EXPORTS_DIR
    chown -R $USER:$USER $APP_DIR $LOG_DIR $DATA_DIR $EXPORTS_DIR
    chmod 755 $APP_DIR $LOG_DIR $DATA_DIR $EXPORTS_DIR
    
    # Switch to the adu user for the rest of the setup
    echo_info "Switching to user: $USER"
    exec sudo -u $USER $0 "$@"
fi

# Ensure we're in the app directory
cd $APP_DIR

# Check for virtual environment
if [ ! -d "$VENV_DIR" ]; then
    echo_info "Creating Python virtual environment..."
    python3 -m venv $VENV_DIR
fi

# Activate virtual environment
source $VENV_DIR/bin/activate

# Install/upgrade dependencies
echo_info "Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt
pip install gunicorn

# Load environment variables
if [ -f ".env" ]; then
    echo_info "Loading environment variables from .env file..."
    export $(cat .env | grep -v '^#' | xargs)
else
    echo_warn "No .env file found. Using default configuration."
    echo_warn "Copy .env.example to .env and configure for production."
fi

# Initialize database if it doesn't exist
if [ ! -f "${ADU_DB_PATH:-$DATA_DIR/adu.db}" ]; then
    echo_info "Initializing database..."
    python3 init_database.py
fi

# Function to start the web application
start_web() {
    echo_info "Starting web application..."
    gunicorn --config gunicorn.conf.py --chdir adu app:app
}

# Function to start the worker
start_worker() {
    echo_info "Starting worker process..."
    cd adu && python3 worker.py
}

# Function to start both services
start_all() {
    echo_info "Starting all services..."
    
    # Start worker in background
    start_worker &
    WORKER_PID=$!
    echo_info "Worker started with PID: $WORKER_PID"
    
    # Start web app in foreground
    start_web &
    WEB_PID=$!
    echo_info "Web app started with PID: $WEB_PID"
    
    # Wait for processes
    wait $WEB_PID $WORKER_PID
}

# Parse command line arguments
case "${1:-all}" in
    web)
        start_web
        ;;
    worker)
        start_worker
        ;;
    all)
        start_all
        ;;
    test)
        echo_info "Running tests..."
        python3 run_tests.py
        ;;
    *)
        echo "Usage: $0 {web|worker|all|test}"
        echo "  web    - Start only the web application"
        echo "  worker - Start only the worker process"
        echo "  all    - Start both web and worker (default)"
        echo "  test   - Run the test suite"
        exit 1
        ;;
esac
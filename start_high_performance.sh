#!/bin/bash

echo "🚀 Starting ADU High-Performance Export System"
echo "=============================================="

# Set environment variables for high performance
export PYTHONPATH=/mnt/nvme/de_images/export_app:$PYTHONPATH
export ADU_DB_PATH=/tmp/adu_high_performance.db
export FLASK_APP=adu.app
export FLASK_ENV=development
export CELERY_BROKER_URL=redis://localhost:6379/0
export CELERY_RESULT_BACKEND=redis://localhost:6379/0

# Set performance optimizations
export PYTHONUNBUFFERED=1
export POLARS_MAX_THREADS=16
export POLARS_MAX_MEMORY_USAGE=32GB

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo "📊 System Information:"
echo "CPU Cores: $(nproc)"
echo "Memory: $(free -h | awk '/^Mem:/ {print $2}')"
echo "Python: $(python3 --version)"
echo ""

# Function to check if a service is running
check_service() {
    if pgrep -f "$1" > /dev/null; then
        echo -e "${GREEN}✅ $2 is running${NC}"
        return 0
    else
        echo -e "${RED}❌ $2 is not running${NC}"
        return 1
    fi
}

# Function to start a service in background
start_service() {
    echo -e "${BLUE}🔄 Starting $2...${NC}"
    nohup $1 > /tmp/$3.log 2>&1 &
    echo $! > /tmp/$3.pid
    sleep 2
    if check_service "$4" "$2"; then
        echo -e "${GREEN}✅ $2 started successfully${NC}"
    else
        echo -e "${RED}❌ Failed to start $2${NC}"
        return 1
    fi
}

# Check prerequisites
echo "🔍 Checking prerequisites..."

# Check if Redis is installed
if ! command -v redis-server &> /dev/null; then
    echo -e "${RED}❌ Redis is not installed. Please install Redis first.${NC}"
    exit 1
fi

# Check if Python packages are installed
python3 -c "import polars, celery, flask, psycopg2, vertica_python" 2>/dev/null
if [ $? -ne 0 ]; then
    echo -e "${YELLOW}⚠️ Installing/upgrading Python dependencies...${NC}"
    pip3 install -r adu/requirements.txt
fi

# Run database migration
echo -e "${BLUE}🗄️ Running database migration...${NC}"
python3 migrate_database.py

# Start services
echo ""
echo -e "${BLUE}🚀 Starting services...${NC}"

# Start Redis if not running
if ! check_service "redis-server" "Redis"; then
    start_service "redis-server --daemonize yes" "Redis" "redis" "redis-server"
fi

# Start Celery worker
if ! check_service "celery.*worker" "Celery Worker"; then
    start_service "celery -A adu.celery_config.celery_app worker --loglevel=info --concurrency=8 --max-tasks-per-child=1000" "Celery Worker" "celery" "celery.*worker"
fi

# Start Flask application
if ! check_service "python.*adu.app" "Flask App"; then
    start_service "python3 -m flask run --host=0.0.0.0 --port=5000" "Flask App" "flask" "python.*flask"
fi

echo ""
echo -e "${GREEN}🎉 ADU High-Performance Export System Started!${NC}"
echo ""
echo "📊 Service Dashboard:"
echo "==================="
check_service "redis-server" "Redis Server"
check_service "celery.*worker" "Celery Worker (8 threads)"
check_service "python.*flask" "Flask Application"

echo ""
echo "🌐 Access Points:"
echo "================"
echo -e "${BLUE}Web Interface:${NC} http://localhost:5000"
echo -e "${BLUE}New High-Performance UI:${NC} http://localhost:5000/templates/index_realtime.html"
echo -e "${BLUE}Job Monitoring:${NC} http://localhost:5000/job/<job_id>"
echo ""

echo "📈 Performance Configuration:"
echo "============================"
echo "• Optimized for 16-core, 128GB systems"
echo "• 8 concurrent table exports"
echo "• 5M row chunks for maximum speed"
echo "• Real-time WebSocket progress updates"
echo "• Comprehensive data integrity validation"
echo "• Memory-efficient streaming processing"
echo ""

echo "📋 Log Files:"
echo "============"
echo "• Redis: /var/log/redis/redis-server.log"
echo "• Celery: /tmp/celery.log"
echo "• Flask: /tmp/flask.log"
echo "• Worker: /tmp/worker.log"
echo ""

echo "🔧 Management Commands:"
echo "======================"
echo "• Stop all: pkill -f 'redis-server|celery|flask'"
echo "• View logs: tail -f /tmp/*.log"
echo "• Monitor jobs: curl http://localhost:5000/api/history"
echo ""

echo -e "${GREEN}System ready for high-performance data exports!${NC}"
echo -e "${YELLOW}Expected performance: 500K+ rows/sec, 200+ tables/hour${NC}"
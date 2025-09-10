#!/bin/bash

# ADU Container Information Display Script
# Shows system capabilities and optimization status

echo "🐳 ADU High-Performance Export Container"
echo "========================================"
echo ""

# System Information
echo "📊 System Specifications:"
echo "├─ CPU Cores: $(nproc)"
echo "├─ Memory Total: $(free -h | awk '/^Mem:/ {print $2}')"
echo "├─ Memory Available: $(free -h | awk '/^Mem:/ {print $7}')"
echo "├─ Disk Space (Exports): $(df -h /app/exports 2>/dev/null | awk 'NR==2 {print $4}' || echo 'N/A')"
echo "└─ Container User: $(whoami)"
echo ""

# Performance Configuration
echo "⚡ Performance Configuration:"
echo "├─ Polars Max Threads: ${POLARS_MAX_THREADS:-16}"
echo "├─ Polars Max Memory: ${POLARS_MAX_MEMORY_USAGE:-32GB}"
echo "├─ Celery Worker Concurrency: ${CELERY_WORKER_CONCURRENCY:-8}"
echo "├─ Celery Max Tasks Per Child: ${CELERY_WORKER_MAX_TASKS_PER_CHILD:-1000}"
echo "└─ Python Path: ${PYTHONPATH:-/app}"
echo ""

# Application Status
echo "🚀 Application Status:"
echo "├─ Database Path: ${ADU_DB_PATH:-/app/database/adu.db}"
echo "├─ Export Path: ${ADU_EXPORT_PATH:-/app/exports}"
echo "├─ Log Path: ${ADU_LOG_PATH:-/app/logs}"
echo "└─ Redis URL: ${CELERY_BROKER_URL:-redis://localhost:6379/0}"
echo ""

# Version Information
echo "📦 Component Versions:"
echo "├─ Python: $(python3 --version 2>/dev/null | cut -d' ' -f2 || echo 'N/A')"
echo "├─ Redis: $(redis-server --version 2>/dev/null | awk '{print $3}' | cut -d'=' -f2 || echo 'N/A')"

# Check Python packages
python3 -c "
import sys
packages = ['polars', 'celery', 'flask', 'psycopg2', 'vertica_python', 'pyarrow']
for pkg in packages:
    try:
        module = __import__(pkg)
        version = getattr(module, '__version__', 'Unknown')
        print(f'├─ {pkg.capitalize()}: {version}')
    except ImportError:
        print(f'├─ {pkg.capitalize()}: Not installed')
" 2>/dev/null

echo "└─ Supervisor: $(supervisord --version 2>/dev/null || echo 'N/A')"
echo ""

# Expected Performance
echo "🎯 Expected Performance (16-core, 128GB optimal):"
echo "├─ Throughput: 500K+ rows/second"
echo "├─ Concurrent Tables: 8 simultaneous"  
echo "├─ Chunk Processing: 32 parallel workers"
echo "├─ Memory Buffer: Up to 32GB"
echo "└─ Database Connections: 16 concurrent"
echo ""

# Optimization Status
echo "🔧 Optimization Status:"
if [ "$(nproc)" -ge 16 ]; then
    echo "✅ CPU: Optimal ($(nproc) cores >= 16)"
else
    echo "⚠️ CPU: Suboptimal ($(nproc) cores < 16 recommended)"
fi

memory_gb=$(free -m | awk '/^Mem:/ {printf "%.0f", $2/1024}')
if [ "$memory_gb" -ge 128 ]; then
    echo "✅ Memory: Optimal (${memory_gb}GB >= 128GB)"
elif [ "$memory_gb" -ge 64 ]; then
    echo "✅ Memory: Good (${memory_gb}GB >= 64GB)"
else
    echo "⚠️ Memory: Limited (${memory_gb}GB < 64GB recommended)"
fi

disk_gb=$(df -BG /app/exports 2>/dev/null | awk 'NR==2 {print $4}' | sed 's/G//' || echo "0")
if [ "$disk_gb" -ge 100 ]; then
    echo "✅ Disk: Adequate (${disk_gb}GB >= 100GB)"
else
    echo "⚠️ Disk: Limited (${disk_gb}GB < 100GB recommended)"
fi

echo ""
echo "🌐 Access Points (once started):"
echo "├─ Web Interface: http://localhost:5000"
echo "├─ High-Performance UI: http://localhost:5000/templates/index_realtime.html"
echo "├─ API Endpoint: http://localhost:5000/api/"
echo "└─ Health Check: http://localhost:5000/health"
echo ""

echo "📋 Quick Commands:"
echo "├─ View logs: tail -f /app/logs/*.log"
echo "├─ Health check: /app/healthcheck.sh"
echo "├─ Service status: supervisorctl status"
echo "└─ Container shell: docker exec -it <container> /bin/bash"
echo ""
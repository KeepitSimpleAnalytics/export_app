#!/bin/bash
set -e

# ADU Container Entrypoint Script
# Handles initialization, environment setup, and service startup

echo "🐳 ADU High-Performance Export Container Starting"
echo "=================================================="

# Function to log with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Trap handler for graceful shutdown
shutdown() {
    log "🛑 Received shutdown signal, stopping services gracefully..."
    
    # Stop supervisor and all managed processes
    if [ -f /app/logs/supervisord.pid ]; then
        supervisorctl -c /etc/supervisor/conf.d/supervisord.conf stop all
        kill -TERM $(cat /app/logs/supervisord.pid) 2>/dev/null || true
    fi
    
    # Give processes time to shut down gracefully
    sleep 5
    
    log "✅ Container shutdown complete"
    exit 0
}

# Set up signal handlers
trap shutdown SIGTERM SIGINT SIGQUIT

# Environment validation
validate_environment() {
    log "🔍 Validating container environment..."
    
    # Check required directories
    for dir in /app/database /app/exports /app/logs; do
        if [ ! -d "$dir" ]; then
            log "📁 Creating directory: $dir"
            mkdir -p "$dir"
        fi
        
        # Ensure proper permissions
        if [ ! -w "$dir" ]; then
            log "⚠️ Warning: Directory $dir is not writable"
        fi
    done
    
    # Check disk space
    available_space=$(df -BG /app/exports | awk 'NR==2 {print $4}' | sed 's/G//')
    if [ "$available_space" -lt 10 ]; then
        log "⚠️ Warning: Low disk space available: ${available_space}GB"
    fi
    
    # Check memory
    total_memory=$(free -m | awk '/^Mem:/ {print $2}')
    if [ "$total_memory" -lt 8192 ]; then
        log "⚠️ Warning: Available memory is ${total_memory}MB, recommended minimum is 8GB"
    fi
    
    log "✅ Environment validation complete"
}

# Initialize application
initialize_application() {
    log "🚀 Initializing ADU application..."
    
    # Set proper permissions
    chown -R adu:adu /app/database /app/exports /app/logs
    
    # Create initial log files
    touch /app/logs/{supervisord.log,redis.log,celery_worker.log,flask.log,health_monitor.log}
    
    # Run database migration if needed
    if [ ! -f /app/database/adu.db ]; then
        log "📊 Initializing database..."
        python3 migrate_database.py
    else
        log "📊 Database exists, checking for migrations..."
        python3 migrate_database.py
    fi
    
    # Verify Python dependencies
    python3 -c "
import sys
try:
    import polars, celery, flask, psycopg2, vertica_python, redis
    print('✅ All Python dependencies verified')
except ImportError as e:
    print(f'❌ Missing dependency: {e}')
    sys.exit(1)
"
    
    log "✅ Application initialization complete"
}

# Performance optimization
optimize_performance() {
    log "⚡ Applying performance optimizations..."
    
    # Set optimal ulimits for high-performance processing
    ulimit -n 65536  # Increase file descriptor limit
    ulimit -u 32768  # Increase process limit
    
    # Memory optimization
    echo "vm.swappiness=10" >> /etc/sysctl.conf 2>/dev/null || true
    echo "vm.dirty_ratio=15" >> /etc/sysctl.conf 2>/dev/null || true
    echo "vm.dirty_background_ratio=5" >> /etc/sysctl.conf 2>/dev/null || true
    
    # Set Python optimizations
    export PYTHONHASHSEED=0
    export MALLOC_ARENA_MAX=4
    
    log "✅ Performance optimizations applied"
}

# Health check setup
setup_health_monitoring() {
    log "🏥 Setting up health monitoring..."
    
    # Create health check endpoint file
    cat > /app/health_status.json << EOF
{
    "status": "starting",
    "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
    "services": {
        "redis": "unknown",
        "celery": "unknown", 
        "flask": "unknown"
    }
}
EOF
    
    chmod 644 /app/health_status.json
    
    log "✅ Health monitoring configured"
}

# Main execution flow
main() {
    log "🎯 Starting ADU container initialization..."
    
    # Run initialization steps
    validate_environment
    initialize_application
    optimize_performance  
    setup_health_monitoring
    
    # Show final system information
    log "📊 Container ready with following configuration:"
    log "   • Python version: $(python3 --version)"
    log "   • Available memory: $(free -h | awk '/^Mem:/ {print $2}')"
    log "   • Available disk: $(df -h /app/exports | awk 'NR==2 {print $4}')"
    log "   • CPU cores: $(nproc)"
    log "   • User: $(whoami)"
    log "   • Working directory: $(pwd)"
    
    # If arguments provided, execute them
    if [ "$#" -gt 0 ]; then
        log "🎯 Executing command: $*"
        exec "$@"
    else
        log "❌ No command provided to entrypoint"
        exit 1
    fi
}

# Ensure we're running as the adu user
if [ "$(whoami)" != "adu" ]; then
    log "⚠️ Warning: Running as $(whoami), expected adu user"
fi

# Execute main function
main "$@"
#!/bin/bash

# ADU Container Health Check Script
# Monitors all internal services and application health

set -e

# Configuration
HEALTH_STATUS_FILE="/app/health_status.json"
LOG_FILE="/app/logs/health_monitor.log"
MONITOR_MODE=false

# Parse command line arguments
if [ "$1" = "--monitor" ]; then
    MONITOR_MODE=true
fi

# Function to log with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Function to update health status
update_health_status() {
    local service=$1
    local status=$2
    local timestamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    
    # Create or update health status file
    if [ ! -f "$HEALTH_STATUS_FILE" ]; then
        cat > "$HEALTH_STATUS_FILE" << EOF
{
    "status": "unknown",
    "timestamp": "$timestamp",
    "services": {}
}
EOF
    fi
    
    # Update the specific service status
    python3 -c "
import json
try:
    with open('$HEALTH_STATUS_FILE', 'r') as f:
        data = json.load(f)
except:
    data = {'services': {}}

data['services']['$service'] = '$status'
data['timestamp'] = '$timestamp'

# Determine overall status
services = data['services']
if all(s == 'healthy' for s in services.values()):
    data['status'] = 'healthy'
elif any(s == 'unhealthy' for s in services.values()):
    data['status'] = 'unhealthy'  
else:
    data['status'] = 'starting'

with open('$HEALTH_STATUS_FILE', 'w') as f:
    json.dump(data, f, indent=2)
"
}

# Check Redis service
check_redis() {
    local status="unhealthy"
    local details=""
    
    # Check if Redis process is running
    if pgrep -f "redis-server" > /dev/null; then
        # Test Redis connectivity
        if timeout 5 redis-cli ping > /dev/null 2>&1; then
            # Test Redis info
            local info=$(timeout 5 redis-cli info server 2>/dev/null | head -5)
            if [ -n "$info" ]; then
                status="healthy"
                details="Redis responding normally"
            else
                details="Redis not responding to INFO command"
            fi
        else
            details="Redis not responding to PING"
        fi
    else
        details="Redis process not running"
    fi
    
    update_health_status "redis" "$status"
    echo "Redis: $status ($details)"
    return $([ "$status" = "healthy" ] && echo 0 || echo 1)
}

# Check Celery worker
check_celery() {
    local status="unhealthy"
    local details=""
    
    # Check if Celery worker processes are running
    local celery_pids=$(pgrep -f "celery.*worker" || true)
    
    if [ -n "$celery_pids" ]; then
        # Count worker processes
        local worker_count=$(echo "$celery_pids" | wc -l)
        
        # Test Celery status through Redis
        if timeout 10 python3 -c "
from celery import Celery
import sys
try:
    app = Celery('adu_export', broker='redis://localhost:6379/0')
    stats = app.control.inspect().stats()
    if stats:
        print('Celery workers responding')
        sys.exit(0)
    else:
        print('No worker responses')
        sys.exit(1)
except Exception as e:
    print(f'Celery check failed: {e}')
    sys.exit(1)
" 2>/dev/null; then
            status="healthy"
            details="$worker_count worker(s) responding"
        else
            details="Workers not responding to inspect"
        fi
    else
        details="No Celery worker processes found"
    fi
    
    update_health_status "celery" "$status"
    echo "Celery: $status ($details)"
    return $([ "$status" = "healthy" ] && echo 0 || echo 1)
}

# Check Flask application
check_flask() {
    local status="unhealthy"
    local details=""
    
    # Check if Flask/Gunicorn processes are running
    if pgrep -f "gunicorn.*adu.app" > /dev/null; then
        # Test HTTP endpoint
        if timeout 10 curl -s -f http://localhost:5000/ > /dev/null 2>&1; then
            # Test API endpoint
            if timeout 10 curl -s -f http://localhost:5000/api/history > /dev/null 2>&1; then
                status="healthy"
                details="Web server responding to HTTP requests"
            else
                details="API endpoints not responding"
            fi
        else
            details="HTTP server not responding"
        fi
    else
        details="Flask/Gunicorn process not running"
    fi
    
    update_health_status "flask" "$status"
    echo "Flask: $status ($details)"
    return $([ "$status" = "healthy" ] && echo 0 || echo 1)
}

# Check system resources
check_resources() {
    local status="healthy"
    local warnings=""
    
    # Check disk space
    local disk_usage=$(df /app/exports | awk 'NR==2 {print $5}' | sed 's/%//')
    if [ "$disk_usage" -gt 90 ]; then
        status="warning"
        warnings="$warnings Disk usage high: ${disk_usage}%"
    fi
    
    # Check memory usage
    local mem_usage=$(free | awk '/^Mem:/ {printf "%.0f", ($3/$2)*100}')
    if [ "$mem_usage" -gt 90 ]; then
        status="warning"
        warnings="$warnings Memory usage high: ${mem_usage}%"
    fi
    
    # Check load average
    local load_avg=$(uptime | awk '{print $10}' | sed 's/,//')
    local cpu_cores=$(nproc)
    local load_threshold=$((cpu_cores * 2))
    
    if [ "$(echo "$load_avg > $load_threshold" | bc 2>/dev/null || echo 0)" = "1" ]; then
        status="warning"
        warnings="$warnings High load average: $load_avg"
    fi
    
    if [ -n "$warnings" ]; then
        echo "Resources: $status ($warnings)"
    else
        echo "Resources: healthy"
    fi
}

# Comprehensive health check
run_health_check() {
    local overall_status=0
    
    echo "🏥 ADU Container Health Check - $(date)"
    echo "========================================="
    
    # Check all services
    check_redis || overall_status=1
    check_celery || overall_status=1
    check_flask || overall_status=1
    
    # Check resources (warnings don't fail health check)
    check_resources
    
    # Show overall status
    if [ $overall_status -eq 0 ]; then
        echo "✅ Overall Status: HEALTHY"
        return 0
    else
        echo "❌ Overall Status: UNHEALTHY"
        return 1
    fi
}

# Monitoring mode (continuous health monitoring)
monitoring_loop() {
    log "🏥 Starting health monitoring service..."
    
    while true; do
        if run_health_check > /dev/null 2>&1; then
            log "✅ Health check passed"
        else
            log "❌ Health check failed - investigating..."
            
            # Log detailed status for troubleshooting
            {
                echo "=== Detailed Health Check ==="
                run_health_check
                echo "=== Process List ==="
                ps aux | grep -E "(redis|celery|gunicorn)" | grep -v grep
                echo "=== Memory Usage ==="
                free -h
                echo "=== Disk Usage ==="
                df -h /app
                echo "=========================="
            } >> "$LOG_FILE"
        fi
        
        # Wait before next check
        sleep 30
    done
}

# Main execution
main() {
    # Ensure log directory exists
    mkdir -p "$(dirname "$LOG_FILE")"
    
    if [ "$MONITOR_MODE" = true ]; then
        monitoring_loop
    else
        # Docker health check mode
        run_health_check
    fi
}

# Execute main function
main
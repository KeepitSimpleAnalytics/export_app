# ADU Export Application - Airgapped Deployment Instructions

## 📦 Image Files

**Generated Image Files:**
- `adu-export-airgapped-20250805-184113.tar` (420MB) - Uncompressed Docker image
- `adu-export-airgapped-20250805-184113.tar.gz` (133MB) - Compressed for transfer

## 🚀 Deployment Steps

### Step 1: Transfer Image to Airgapped Environment

**Option A: Transfer compressed image (recommended)**
```bash
# Transfer the compressed file (133MB)
scp adu-export-airgapped-20250805-184113.tar.gz user@airgapped-server:/path/to/imports/

# On airgapped server - decompress
cd /path/to/imports/
gunzip adu-export-airgapped-20250805-184113.tar.gz
```

**Option B: Transfer uncompressed image**
```bash
# Transfer the uncompressed file (420MB)
scp adu-export-airgapped-20250805-184113.tar user@airgapped-server:/path/to/imports/
```

### Step 2: Load Docker Image

```bash
# Load the image into Docker
docker load -i adu-export-airgapped-20250805-184113.tar

# Verify the image was loaded
docker images | grep adu-export
```

**Expected output:**
```
adu-export   airgapped   15da8233efbf   2 minutes ago   1.37GB
```

### Step 3: Create Data Directories

```bash
# Create required directories
mkdir -p /data/adu-export/{exports,database,logs}

# Set permissions (adjust as needed for your environment)
chmod 755 /data/adu-export
chmod 755 /data/adu-export/{exports,database,logs}
```

### Step 4: Deploy Container

#### Basic Deployment
```bash
docker run -d \
  --name adu-export \
  -p 5000:5000 \
  -v /data/adu-export/exports:/app/exports \
  -v /data/adu-export/database:/app/adu/data \
  -v /data/adu-export/logs:/app/logs \
  adu-export:airgapped
```

#### Production Deployment (16 vCPU + 128GB RAM)
```bash
docker run -d \
  --name adu-export-prod \
  --restart unless-stopped \
  -p 5000:5000 \
  -e OMP_NUM_THREADS=16 \
  -e POLARS_MAX_THREADS=16 \
  -e FLASK_DEBUG=False \
  --memory=32g \
  --cpus=16 \
  -v /data/adu-export/exports:/app/exports \
  -v /data/adu-export/database:/app/adu/data \
  -v /data/adu-export/logs:/app/logs \
  adu-export:airgapped
```

### Step 5: Verify Deployment

```bash
# Check container status
docker ps | grep adu-export

# Check logs
docker logs adu-export

# Test web interface
curl http://localhost:5000/

# Check health endpoint
curl http://localhost:5000/health
```

**Expected health response:**
```json
{
  "status": "healthy",
  "worker_running": true,
  "database_connected": true,
  "timestamp": "2025-08-05T18:41:13Z"
}
```

## 🔧 Configuration

### Environment Variables

```bash
# Optional configuration via environment variables
docker run -d \
  --name adu-export \
  -p 5000:5000 \
  -e FLASK_DEBUG=False \
  -e SECRET_KEY=your-secret-key-here \
  -e OMP_NUM_THREADS=16 \
  -e POLARS_MAX_THREADS=16 \
  -v /data/adu-export/exports:/app/exports \
  -v /data/adu-export/database:/app/adu/data \
  adu-export:airgapped
```

### Performance Tuning for Different Hardware

#### 8 vCPU + 64GB RAM
```bash
docker run -d \
  --name adu-export \
  -p 5000:5000 \
  -e OMP_NUM_THREADS=8 \
  -e POLARS_MAX_THREADS=8 \
  --memory=16g \
  --cpus=8 \
  -v /data/adu-export/exports:/app/exports \
  -v /data/adu-export/database:/app/adu/data \
  adu-export:airgapped
```

#### 32 vCPU + 256GB RAM (High-end)
```bash
docker run -d \
  --name adu-export \
  -p 5000:5000 \
  -e OMP_NUM_THREADS=32 \
  -e POLARS_MAX_THREADS=32 \
  --memory=64g \
  --cpus=32 \
  -v /data/adu-export/exports:/app/exports \
  -v /data/adu-export/database:/app/adu/data \
  adu-export:airgapped
```

## 📊 First Export Test

### Access Web Interface
1. Open browser to `http://your-server:5000`
2. Navigate to "Create Export Job"
3. Enter your database connection details
4. Select a small test table first
5. Click "Start Export"

### Monitor Progress
```bash
# Watch logs in real-time
docker logs -f adu-export

# Check export directory
ls -la /data/adu-export/exports/

# Monitor container resources
docker stats adu-export
```

## 🔍 Troubleshooting

### Container Won't Start
```bash
# Check container logs
docker logs adu-export

# Check if ports are available
netstat -tulpn | grep 5000

# Verify image was loaded correctly
docker images | grep adu-export
```

### Web Interface Not Accessible
```bash
# Check if Flask app is running
docker exec adu-export ps aux | grep flask

# Check if worker is running
docker exec adu-export ps aux | grep worker

# Test internal connectivity
docker exec adu-export curl http://localhost:5000/health
```

### Database Connection Issues
```bash
# Test database connectivity from container
docker exec -it adu-export python3 -c "
import psycopg2
try:
    conn = psycopg2.connect(
        host='your-db-host',
        port=5432,
        user='your-username',
        password='your-password',
        database='your-database'
    )
    print('Database connection successful')
    conn.close()
except Exception as e:
    print(f'Database connection failed: {e}')
"
```

### Jobs Stuck in Queue
```bash
# Check if worker process is running
docker exec adu-export ps aux | grep "adu.worker"

# Restart container if needed
docker restart adu-export

# Check worker logs
docker logs adu-export | grep -i worker
```

## 📁 File Structure After Deployment

```
/data/adu-export/
├── exports/                    # Export output directory
│   ├── schema_table1/         # Clean table directories
│   ├── schema_table2/
│   └── .archive/              # Job history archives
├── database/                  # Application database
│   └── adu.db                # SQLite database file
└── logs/                      # Application logs
    ├── app.log               # Flask application logs
    └── worker.log            # Worker process logs
```

## 🎯 Quick Start Checklist

- [ ] Transfer image file to airgapped environment
- [ ] Load image: `docker load -i adu-export-airgapped-*.tar`
- [ ] Create data directories: `/data/adu-export/{exports,database,logs}`
- [ ] Run container with volume mounts
- [ ] Verify container is running: `docker ps`
- [ ] Check health: `curl http://localhost:5000/health`
- [ ] Access web interface: `http://localhost:5000`
- [ ] Test with small table export
- [ ] Monitor logs: `docker logs -f adu-export`

## 🚀 Ready for Production!

The image contains:
- ✅ **Ultra-simple architecture** - No supervisor complexity
- ✅ **Multi-threaded performance** - 12 table threads + 8 chunk workers
- ✅ **Clean export organization** - No job ID clutter
- ✅ **Greenplum optimizations** - Ready for 11TB exports
- ✅ **Airgapped security** - No external dependencies
- ✅ **Comprehensive logging** - Full visibility into operations
- ✅ **Hardware optimization** - Auto-detects and uses all available CPU cores

**Image Details:**
- Base: Python 3.12 slim
- Size: 1.37GB (420MB compressed transfer)
- Architecture: linux/amd64
- Optimized for: 16 vCPU + 128GB RAM systems
- Ready for: PostgreSQL, Greenplum, Vertica databases

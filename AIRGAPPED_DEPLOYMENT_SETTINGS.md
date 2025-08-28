# ADU Airgapped Deployment Guide
## Complete Settings and Commands for Port 8504
### 🔧 UPDATED: Fixed PyArrow & Schema Issues + Job Cancellation

---

## 1. Environment File (.env)

Create `/data/adu/.env` with the following content:

```bash
# Flask Configuration
SECRET_KEY=your-secure-random-secret-key-change-this
FLASK_DEBUG=false
FLASK_RUN_PORT=8504

# Database Configuration (will be mounted to /data/adu/adu.db)
ADU_DB_PATH=/data/adu/adu.db

# Application Paths
DEFAULT_EXPORT_PATH=/app/exports

# Production Settings
PYTHONPATH=/app
```

### Generate a Secure SECRET_KEY:
```bash
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
```

---

## 2. Directory Structure

Create the following directories on your host system:

```bash
mkdir -p /data/adu/data
mkdir -p /data/adu/exports
mkdir -p /data/adu/logs
```

---

## 3. Docker Image - **LATEST VERSION WITH PURE POLARS**

**NEWEST Image File:** `adu-pure-polars-v6-20250806-182935.tar.gz` (175MB)

### ✅ Fixed Issues:
- **Added PyArrow dependency** - Resolves "No module named 'pyarrow'" errors
- **Schema-based data type handling** - Gets column metadata from database (PostgreSQL/Greenplum/Vertica)
- **String length protection** - Automatically truncates long strings to prevent schema overflow
- **Better Parquet compatibility** - Handles problematic data types properly
- **🔧 SYNTAX ERROR FIXED** - Resolved unmatched parenthesis causing startup failures
- **🔧 PORT CONFIGURATION FIXED** - Now properly uses FLASK_RUN_PORT environment variable (default: 8504)
- **🔧 PURE POLARS IMPLEMENTATION** - Eliminated Pandas dependencies, uses pure Polars for all data operations

### 🆕 NEW: Job Cancellation Feature:
- **Cancel running jobs** - Stop jobs that are in 'queued' or 'running' status
- **Graceful cancellation** - Stops processing new tables and cancels remaining work
- **UI integration** - Cancel buttons in Job History and Job Details pages
- **API endpoint** - `POST /api/job/<job_id>/cancel` for programmatic cancellation

### 🔧 Schema & Data Type Improvements:
- **Conservative type preservation** - Only converts data types when absolutely necessary
- **Smart fallback handling** - Preserves original types when schema detection works
- **No more "convert everything to strings"** - Eliminates problematic string conversion fallbacks
- **Robust error handling** - Graceful schema conflict resolution without data type destruction
- **Pure Polars data pipeline** - No mixing with Pandas, consistent performance and behavior

### Load the New Image:
```bash
docker load < adu-pure-polars-v6-20250806-182935.tar.gz
```

---

## 4. Docker Run Command

### Primary Command (with --network host):
```bash
docker run -d --name adu-app \
  -e PYTHONPATH=/app \
  --env-file /data/adu/.env \
  --network host \
  -e FLASK_RUN_PORT=8504 \
  -e ADU_DB_PATH=/data/adu/adu.db \
  -v /data/adu:/data/adu \
  -v /data/adu/exports:/app/exports \
  -v /data/adu/logs:/tmp \
  --restart unless-stopped \
  adu-export:pure-polars-v6
```

### Alternative Command (with explicit port mapping):
```bash
docker run -d --name adu-app \
  -e PYTHONPATH=/app \
  --env-file /data/adu/.env \
  -p 8504:8504 \
  -e FLASK_RUN_PORT=8504 \
  -e ADU_DB_PATH=/data/adu/adu.db \
  -v /data/adu:/data/adu \
  -v /data/adu/exports:/app/exports \
  -v /data/adu/logs:/tmp \
  --restart unless-stopped \
  adu-export:pure-polars-v6
```

---

## 5. Container Internal Paths

**Important:** These are the paths inside the container:

- **Database:** `/tmp/adu.db` (default) or custom via `ADU_DB_PATH`
- **Worker Logs:** `/tmp/worker.log`
- **Exports:** `/app/exports`
- **Application:** `/app`

---

## 6. Volume Mappings Explained

| Host Path | Container Path | Purpose |
|-----------|----------------|---------|
| `/data/adu/.env` | Environment variables | Flask configuration |
| `/data/adu` | `/data/adu` | Database persistence |
| `/data/adu/exports` | `/app/exports` | Export files storage |
| `/data/adu/logs` | `/tmp` | Worker logs persistence |

---

## 7. Verification Commands

### Check Container Status:
```bash
docker ps
docker logs adu-app
```

### Check Application Health:
```bash
curl http://localhost:8504/
```

### View Worker Logs:
```bash
cat /data/adu/logs/worker.log
```

### Check Database:
```bash
ls -la /data/adu/adu.db
```

---

## 8. Troubleshooting

### Container Won't Start:
```bash
# Check logs
docker logs adu-app

# Check environment file
cat /data/adu/.env

# Check permissions
ls -la /data/adu/
```

### Port Issues:
```bash
# Check if port 8504 is in use
netstat -tulpn | grep 8504

# Check container port binding
docker port adu-app
```

### Database Issues:
```bash
# Check database file exists
ls -la /data/adu/adu.db

# Check database permissions
stat /data/adu/adu.db
```

---

## 9. Application URLs & Job Management

- **Main Application:** http://localhost:8504/
- **Job History:** http://localhost:8504/history
- **Logs Viewer:** http://localhost:8504/logs
- **API Health:** http://localhost:8504/api/history

### 🆕 Job Cancellation:
- **History Page:** Click "Cancel" link next to running/queued jobs
- **Job Details Page:** Click "Cancel Job" button for running/queued jobs
- **API Endpoint:** `POST /api/job/<job_id>/cancel`

**Example API Usage:**
```bash
curl -X POST http://localhost:8504/api/job/your-job-id/cancel \
  -H "Content-Type: application/json"
```

---

## 10. Container Management

### Stop Container:
```bash
docker stop adu-app
```

### Start Container:
```bash
docker start adu-app
```

### Remove Container:
```bash
docker stop adu-app
docker rm adu-app
```

### Update Image:
```bash
# Stop and remove old container
docker stop adu-app && docker rm adu-app

# Load new image
docker load < new-image.tar.gz

# Run with same command as above
```

---

## 11. Security Notes

- **SECRET_KEY:** Generate a unique key for production
- **Database:** SQLite file stored on host for persistence
- **Logs:** Worker logs accessible via web interface and host filesystem
- **Network:** Uses host networking or explicit port mapping for 8504

---

## 12. File Checklist

Before deployment, ensure you have:

- [ ] `adu-pure-polars-v6-20250806-182935.tar.gz` (LATEST with pure Polars implementation)
- [ ] `/data/adu/.env` (Environment file with correct settings)
- [ ] `/data/adu/` directories created
- [ ] Port 8504 available
- [ ] Docker installed and running

---

## 13. Quick Start

1. Create directories: `mkdir -p /data/adu/{data,exports,logs}`
2. Create `.env` file with settings above
3. Load image: `docker load < adu-pure-polars-v6-20250806-182935.tar.gz`
4. Run container with provided Docker command
5. Access application at http://localhost:8504/

---

## 14. What Was Fixed

### 🐛 Issues Resolved:
1. **PyArrow Missing**: Added `pyarrow` to requirements.txt
2. **String Length Overflow**: Added automatic string truncation (5000 char limit)
3. **Schema Inference Problems**: Now reads column metadata from database schema
4. **Data Type Compatibility**: Proper mapping from DB types to Parquet-safe types
5. **🔧 Syntax Error**: Fixed unmatched parenthesis causing container startup failure
6. **🔧 Port Configuration**: Fixed hardcoded port 5000, now respects FLASK_RUN_PORT environment variable  
7. **🔧 Pure Polars Implementation**: Eliminated all Pandas dependencies, uses pure Polars for database reads

### 🆕 NEW Features:
8. **Job Cancellation**: Cancel running or queued jobs gracefully
9. **Enhanced UI**: Cancel buttons in History and Job Details pages
10. **API Endpoint**: Programmatic job cancellation support

### 🔧 Technical Improvements:
- **Schema-based processing**: Queries `information_schema.columns` for PostgreSQL/Greenplum
- **Smart data type mapping**: Handles VARCHAR lengths, NUMERIC precision, JSON columns
- **Conservative type conversion**: Only converts when absolutely necessary, preserves original types
- **Eliminated string fallbacks**: No more "convert everything to strings" workarounds
- **Better error handling**: Graceful handling of problematic columns without destroying data types  
- **Pure Polars pipeline**: Consistent performance and behavior, no Pandas/Polars mixing issues
- **Cancellation checks**: Worker processes check for cancellation throughout execution
- **Thread-safe cancellation**: Properly cancels parallel table processing

---

## 15. Job Cancellation Details

### When You Can Cancel:
- **Queued jobs**: Before they start processing
- **Running jobs**: While they're actively processing tables

### What Happens When You Cancel:
1. Job status is immediately set to "cancelled"
2. No new tables will be processed
3. Currently processing tables will complete their current chunk
4. Remaining queued tables are skipped
5. Partial exports are preserved where available

### Cancellation Methods:
- **Web UI**: Use cancel buttons in History or Job Details pages
- **API**: Send POST request to `/api/job/<job_id>/cancel`
- **Status**: Cancelled jobs show as "cancelled" status

---

**Image Details:**
- Name: `adu-export:pure-polars-v6`
- Size: 175MB (compressed)
- Build Date: August 6, 2025 18:29
- Syntax Fixes: ✅ Applied (unmatched parenthesis fixed)
- Port Configuration: ✅ Fixed (respects FLASK_RUN_PORT environment variable)
- Pure Polars: ✅ Implemented (no Pandas dependencies, consistent data processing)
- Import Fixes: ✅ Applied (absolute imports)
- Directory: ✅ Cleaned (unnecessary files removed)
- PyArrow: ✅ Included
- Schema Detection: ✅ Implemented
- String Protection: ✅ Active
- Data Type Preservation: ✅ Conservative approach, no unnecessary string conversion
- Job Cancellation: ✅ Available

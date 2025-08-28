# ADU Export Application - Complete Documentation

## 📋 Table of Contents
1. [Overview](#overview)
2. [Export Organization System](#export-organization-system)
3. [Threading & Performance](#threading--performance)
4. [Chunk Processing](#chunk-processing)
5. [Database Support](#database-support)
6. [Configuration Options](#configuration-options)
7. [Deployment Guide](#deployment-guide)
8. [API Reference](#api-reference)
9. [Troubleshooting](#troubleshooting)
10. [Performance Tuning](#performance-tuning)

---

## 🎯 Overview

The ADU (Automated Data Unloader) Export Application is a high-performance, multi-threaded data export system designed for enterprise-scale database exports. It supports exporting terabytes of data from PostgreSQL, Greenplum, and Vertica databases to Parquet format with intelligent organization and performance optimization.

### Key Features
- **Multi-threaded exports** with configurable parallelism
- **Intelligent chunk processing** for large tables
- **Clean export organization** with automatic job cleanup
- **Real-time progress tracking** and logging
- **Enterprise database support** (PostgreSQL, Greenplum, Vertica)
- **Optimized for airgapped environments**
- **Web-based interface** with REST API

---

## 📁 Export Organization System

### Current Implementation: Strategy 1 + 4 Hybrid

The application uses a sophisticated export organization system that provides clean directory structures while preserving job history and handling conflicts intelligently.

#### Phase 1: During Export (Temporary Workspace)
```
exports/
└── .temp/
    └── job_12345/              # Temporary job workspace
        ├── schema_table_1/     # Table-specific directories
        │   ├── part_0001.parquet
        │   ├── part_0002.parquet
        │   └── part_NNNN.parquet
        └── schema_table_2/
            └── part_0001.parquet
```

#### Phase 2: After Job Completion (Clean Organization)
```
exports/
├── schema_table_1/             # Clean final structure
│   ├── part_0001.parquet
│   ├── part_0002.parquet
│   └── _export_metadata.json
├── schema_table_2/
│   └── part_0001.parquet
├── schema_table_1_v2/          # Conflict resolution
└── .archive/
    └── jobs/
        └── job_12345_2025-08-05_17-55-21.json
```

### Export Organization Features

#### 1. Temporary Workspace Protection
- All exports start in `.temp/job_id/` directories
- Prevents incomplete exports from appearing in final structure
- Atomic move operations ensure data consistency
- Failed jobs don't clutter the export directory

#### 2. Intelligent Conflict Resolution
When a table export would conflict with existing data:

**Version Numbering:**
```
exports/
├── employees/           # Original export
├── employees_v2/        # Second export (conflict)
├── employees_v3/        # Third export (conflict)
└── employees_2025-08-05/ # Timestamped if multiple same day
```

**Conflict Detection:**
- Checks for existing table directories
- Preserves all previous exports
- No data loss from concurrent jobs
- Clear versioning for easy identification

#### 3. Job Metadata Archiving
```json
{
  "job_id": "806cfa9b-6d17-4263-92cc-44b06dd7ee82",
  "timestamp": "2025-08-05_17-55-21",
  "database": {
    "type": "postgresql",
    "host": "ml-docker",
    "port": 55432,
    "username": "postgres"
  },
  "tables_exported": [
    {
      "table_name": "public.categories",
      "row_count": 8,
      "export_path": "/app/exports/public_categories",
      "file_count": 1,
      "compression": "snappy",
      "export_time_seconds": 0.5
    }
  ],
  "performance": {
    "total_duration_seconds": 0.8,
    "tables_processed": 1,
    "total_rows_exported": 8,
    "threading_config": {
      "table_threads": 12,
      "chunk_workers": 8
    }
  }
}
```

#### 4. Automatic Cleanup
- Removes temporary directories after successful organization
- Updates database paths to point to final locations
- Maintains referential integrity
- Logs all organization operations

### Configuration Options

```python
export_organization = {
    'strategy': 'clean_with_archive',     # clean_with_archive, direct, schema_first
    'conflict_resolution': 'version',     # version, timestamp, overwrite
    'preserve_job_history': True,
    'auto_cleanup_temp': True,
    'archive_completed_jobs': True
}
```

---

## 🚀 Threading & Performance

### Multi-Level Parallelism Architecture

The application implements a sophisticated two-tier threading system for maximum performance:

#### Level 1: Table-Level Parallelism
- **Default:** 12 concurrent table exports
- **Auto-scaling:** Based on CPU cores (`min(12, cpu_count())`)
- **Purpose:** Process multiple tables simultaneously
- **Ideal for:** Many small-to-medium tables

#### Level 2: Chunk-Level Parallelism  
- **Default:** 8 concurrent chunk workers per large table
- **Purpose:** Split large tables into parallel chunks
- **Ideal for:** Tables with millions/billions of rows

### Threading Configuration

#### Automatic CPU Detection
```python
import multiprocessing

# Default threading configuration
cpu_count = multiprocessing.cpu_count()
table_threads = min(12, cpu_count)           # Table-level parallelism
chunk_workers = min(8, max(2, cpu_count // 2)) # Chunk-level parallelism
```

#### Hardware-Specific Optimizations

**For 16 vCPU + 128GB RAM systems:**
```python
# Optimized configuration
table_threads = 12        # Process 12 tables concurrently
chunk_workers = 8         # 8 parallel chunks per large table
chunk_size = 1000000      # 1M rows per chunk
```

**Environment Variables:**
```bash
export OMP_NUM_THREADS=16
export POLARS_MAX_THREADS=16
```

#### Threading Decision Matrix

| Table Size | Strategy | Threads Used | Example |
|------------|----------|--------------|---------|
| < 100K rows | Single file | 1 table thread | Small lookup tables |
| 100K - 1M rows | Single file | 1 table thread | Medium tables |
| > 1M rows | Chunked export | 1 table + 8 chunk workers | Large fact tables |
| Multiple large tables | Hybrid | 12 table + 8 chunk workers each | Enterprise datasets |

### Performance Benefits

**Before Threading (Single-threaded):**
- 1 table at a time
- Linear processing
- CPU underutilization
- Long export times for large datasets

**After Threading (Multi-level):**
- 12 tables + 8 chunks simultaneously
- Up to 96 parallel operations (12 × 8)
- Full CPU utilization
- Dramatically reduced export times

---

## 🧩 Chunk Processing

### Overview
Chunk processing splits large tables into manageable pieces that can be processed in parallel, dramatically improving performance for tables with millions or billions of rows.

### Chunk Size Configuration

#### Default Chunk Size: 1,000,000 rows
```python
# Configurable chunk size
chunk_size = 1000000  # 1 million rows per chunk
```

#### Chunk Size Selection Guidelines

| Table Size | Recommended Chunk Size | Reasoning |
|------------|----------------------|-----------|
| 1M - 10M rows | 1,000,000 | Balanced memory usage |
| 10M - 100M rows | 2,000,000 | Reduce overhead |
| 100M - 1B rows | 5,000,000 | Optimize for large datasets |
| > 1B rows | 10,000,000 | Minimize chunk count |

#### Memory Considerations
```python
# Memory estimation per chunk
estimated_memory_mb = chunk_size * average_row_size_bytes / 1024 / 1024

# For 1M rows with 1KB average row size
# Memory usage ≈ 1000MB per chunk
# With 8 chunk workers: 8GB memory usage
```

### Chunking Strategies

#### 1. Range-Based Chunking (Primary Keys)
```sql
-- Chunk 1: rows 1-1,000,000
SELECT * FROM large_table WHERE id BETWEEN 1 AND 1000000

-- Chunk 2: rows 1,000,001-2,000,000  
SELECT * FROM large_table WHERE id BETWEEN 1000001 AND 2000000

-- Chunk N: remaining rows
SELECT * FROM large_table WHERE id BETWEEN N000001 AND MAX(id)
```

**Advantages:**
- Even distribution
- Efficient with indexed primary keys
- Predictable performance

**Requirements:**
- Numeric primary key
- Sequential or near-sequential values
- Primary key index

#### 2. Offset-Based Chunking (Fallback)
```sql
-- Chunk 1
SELECT * FROM large_table ORDER BY primary_key LIMIT 1000000 OFFSET 0

-- Chunk 2  
SELECT * FROM large_table ORDER BY primary_key LIMIT 1000000 OFFSET 1000000

-- Chunk N
SELECT * FROM large_table ORDER BY primary_key LIMIT 1000000 OFFSET N000000
```

**Advantages:**
- Works with any primary key type
- Handles non-sequential keys
- Consistent ordering

**Disadvantages:**
- Higher memory usage for large offsets
- Slower performance on very large tables

### Chunk Processing Workflow

#### 1. Table Analysis
```python
def analyze_table_for_chunking(table_name, connection):
    """
    Analyze table to determine chunking strategy
    """
    # Get row count
    row_count = get_table_row_count(table_name, connection)
    
    # Get primary key information
    primary_key = get_primary_key_info(table_name, connection)
    
    # Determine chunking strategy
    if row_count > 1000000:  # 1M row threshold
        if is_numeric_sequential_pk(primary_key):
            return "range_based_chunking"
        else:
            return "offset_based_chunking"
    else:
        return "single_file_export"
```

#### 2. Chunk Generation
```python
def generate_chunks(table_name, row_count, chunk_size, primary_key):
    """
    Generate chunk specifications for parallel processing
    """
    chunks = []
    
    if chunking_strategy == "range_based":
        # Find min/max primary key values
        min_id, max_id = get_pk_range(table_name, primary_key)
        
        # Calculate chunk boundaries
        range_per_chunk = (max_id - min_id + 1) // (row_count // chunk_size + 1)
        
        for i in range(0, row_count, chunk_size):
            chunk_start = min_id + (i // chunk_size) * range_per_chunk
            chunk_end = min(chunk_start + range_per_chunk - 1, max_id)
            
            chunks.append({
                'chunk_id': i // chunk_size,
                'start_id': chunk_start,
                'end_id': chunk_end,
                'sql_where': f"{primary_key} BETWEEN {chunk_start} AND {chunk_end}"
            })
    
    return chunks
```

#### 3. Parallel Chunk Processing
```python
def process_chunks_parallel(table_name, chunks, connection_params):
    """
    Process multiple chunks in parallel using ThreadPoolExecutor
    """
    with ThreadPoolExecutor(max_workers=8) as executor:
        # Submit all chunks for processing
        futures = []
        for chunk in chunks:
            future = executor.submit(
                export_table_chunk,
                table_name,
                chunk,
                connection_params
            )
            futures.append(future)
        
        # Wait for all chunks to complete
        for future in as_completed(futures):
            chunk_result = future.result()
            log_chunk_completion(chunk_result)
```

### Chunk Output Format

#### File Naming Convention
```
exports/
└── large_table/
    ├── part_0001.parquet    # Chunk 1: rows 1-1,000,000
    ├── part_0002.parquet    # Chunk 2: rows 1,000,001-2,000,000
    ├── part_0003.parquet    # Chunk 3: rows 2,000,001-3,000,000
    └── part_NNNN.parquet    # Chunk N: remaining rows
```

#### Metadata Tracking
```json
{
  "table_name": "large_table",
  "total_rows": 15750000,
  "chunk_strategy": "range_based",
  "chunk_size": 1000000,
  "chunks": [
    {
      "file": "part_0001.parquet",
      "chunk_id": 1,
      "rows": 1000000,
      "start_id": 1,
      "end_id": 1000000,
      "compression": "snappy",
      "size_mb": 245.7
    },
    {
      "file": "part_0002.parquet", 
      "chunk_id": 2,
      "rows": 1000000,
      "start_id": 1000001,
      "end_id": 2000000,
      "compression": "snappy",
      "size_mb": 243.2
    }
  ]
}
```

### Performance Optimization

#### Greenplum-Specific Optimizations
```python
# Optimized for Greenplum's distributed architecture
greenplum_config = {
    'chunk_size': 5000000,      # Larger chunks for distributed queries
    'compression': 'zstd:3',    # Better compression ratio
    'parallel_workers': 8,      # Match segment count
    'connection_pool_size': 16  # Higher connection pool
}
```

#### Memory Management
```python
# Memory-conscious chunk processing
def process_chunk_with_memory_management(chunk):
    try:
        # Load chunk data
        df = load_chunk_data(chunk)
        
        # Process in smaller batches if needed
        if len(df) > 500000:  # 500K row memory threshold
            return process_chunk_in_batches(df, batch_size=100000)
        else:
            return process_chunk_direct(df)
            
    finally:
        # Explicit memory cleanup
        del df
        gc.collect()
```

---

## 💾 Database Support

### Supported Database Systems

#### 1. PostgreSQL
```python
postgresql_config = {
    'driver': 'psycopg2',
    'connection_string': 'postgresql://user:password@host:port/database',
    'features': {
        'chunk_processing': True,
        'range_based_chunking': True,
        'transaction_isolation': True,
        'connection_pooling': True
    }
}
```

#### 2. Greenplum
```python
greenplum_config = {
    'driver': 'psycopg2',  # Uses PostgreSQL protocol
    'connection_string': 'postgresql://user:password@host:port/database',
    'optimizations': {
        'distributed_queries': True,
        'segment_aware_chunking': True,
        'compression': 'zstd:3',
        'chunk_size': 5000000
    }
}
```

#### 3. Vertica
```python
vertica_config = {
    'driver': 'vertica_python',
    'connection_string': 'vertica://user:password@host:port/database',
    'features': {
        'columnar_optimization': True,
        'projection_awareness': True,
        'cluster_functions': True
    }
}
```

### Connection Management

#### Connection Pooling
```python
class DatabaseConnectionManager:
    def __init__(self, config):
        self.config = config
        self.connection_pool = {
            'max_connections': 20,
            'min_connections': 5,
            'connection_timeout': 30,
            'idle_timeout': 600
        }
    
    def get_connection(self):
        """Get connection from pool or create new one"""
        pass
    
    def return_connection(self, conn):
        """Return connection to pool"""
        pass
```

#### Database-Specific Optimizations

**PostgreSQL/Greenplum:**
```sql
-- Query optimization for chunk processing
SET work_mem = '1GB';
SET max_parallel_workers_per_gather = 4;
SET enable_partitionwise_join = on;
SET enable_partitionwise_aggregate = on;
```

**Vertica:**
```sql
-- Vertica-specific optimizations
SET SESSION RESOURCE_POOL = 'export_pool';
SET SESSION LABEL = 'ADU_EXPORT';
```

---

## ⚙️ Configuration Options

### Application Configuration

#### Environment Variables
```bash
# Flask Configuration
FLASK_APP=adu.app
FLASK_DEBUG=False
SECRET_KEY=simple-airgapped-secret

# Database Configuration  
DB_TYPE=postgresql
DB_HOST=localhost
DB_PORT=5432
DB_NAME=database
DB_USERNAME=user
DB_PASSWORD=password

# Performance Configuration
OMP_NUM_THREADS=16
POLARS_MAX_THREADS=16
PYTHONPATH=/app

# Export Configuration
EXPORT_BASE_PATH=/app/exports
CHUNK_SIZE=1000000
TABLE_THREADS=12
CHUNK_WORKERS=8
```

#### Runtime Configuration
```python
# config.py
class ExportConfig:
    # Threading configuration
    TABLE_THREADS = 12
    CHUNK_WORKERS = 8
    CHUNK_SIZE = 1000000
    
    # Export organization
    EXPORT_STRATEGY = 'clean_with_archive'
    CONFLICT_RESOLUTION = 'version'
    PRESERVE_JOB_HISTORY = True
    AUTO_CLEANUP_TEMP = True
    
    # Performance tuning
    CONNECTION_POOL_SIZE = 20
    QUERY_TIMEOUT = 3600  # 1 hour
    MEMORY_LIMIT_MB = 4096
    
    # Compression settings
    COMPRESSION = 'snappy'  # snappy, gzip, zstd
    COMPRESSION_LEVEL = 3
```

### Job-Specific Configuration
```json
{
  "job_config": {
    "database": {
      "type": "postgresql",
      "host": "ml-docker",
      "port": 55432,
      "username": "postgres",
      "password": "password",
      "database": "northwind"
    },
    "export_settings": {
      "format": "parquet",
      "compression": "snappy",
      "chunk_size": 1000000,
      "validate_exports": true
    },
    "threading": {
      "table_threads": 12,
      "chunk_workers": 8,
      "max_concurrent_chunks": 96
    },
    "tables": [
      {
        "schema": "public",
        "table": "large_fact_table",
        "where_clause": "created_date >= '2024-01-01'",
        "chunk_override": {
          "chunk_size": 2000000,
          "chunk_workers": 16
        }
      }
    ]
  }
}
```

---

## 🚀 Deployment Guide

### Docker Deployment (Recommended)

#### Build the Image
```bash
# Build ultra-simple image
docker build -t adu-export:latest -f Dockerfile.ultra-simple .
```

#### Run the Container
```bash
# Basic deployment
docker run -d \
  --name adu-export \
  -p 5000:5000 \
  -v $(pwd)/exports:/app/exports \
  -v $(pwd)/adu/data:/app/adu/data \
  adu-export:latest

# Production deployment with environment variables
docker run -d \
  --name adu-export-prod \
  -p 5000:5000 \
  -e FLASK_DEBUG=False \
  -e OMP_NUM_THREADS=16 \
  -e POLARS_MAX_THREADS=16 \
  -v /data/exports:/app/exports \
  -v /data/database:/app/adu/data \
  -v /logs:/app/logs \
  adu-export:latest
```

#### Docker Compose (Production)
```yaml
# docker-compose.yml
version: '3.8'

services:
  adu-export:
    image: adu-export:latest
    ports:
      - "5000:5000"
    environment:
      - FLASK_DEBUG=False
      - OMP_NUM_THREADS=16
      - POLARS_MAX_THREADS=16
    volumes:
      - ./exports:/app/exports
      - ./data:/app/adu/data
      - ./logs:/app/logs
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:5000/"]
      interval: 30s
      timeout: 10s
      retries: 3
```

### Local Development

#### Setup Environment
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate  # Windows

# Install dependencies
pip install -r adu/requirements.txt

# Initialize database
python init_database.py

# Set environment variables
export FLASK_APP=adu.app
export PYTHONPATH=.
```

#### Run Development Server
```bash
# Start Flask app
python -m flask run --host=0.0.0.0 --port=5000 &

# Start worker process
python -m adu.worker &
```

### Production Considerations

#### Resource Requirements
```yaml
# Minimum requirements
CPU: 4 cores
RAM: 8GB
Storage: 100GB+ for exports
Network: 1Gbps

# Recommended for 11TB exports
CPU: 16+ cores
RAM: 128GB+
Storage: 20TB+ SSD
Network: 10Gbps
```

#### Security (Airgapped Environment)
- No external network access required
- Local authentication only
- File-based session management
- No encryption overhead
- Direct database connections

#### Monitoring
```bash
# Monitor container resources
docker stats adu-export

# Check export progress
tail -f logs/worker.log

# Monitor disk usage
df -h /data/exports
```

---

## 🔧 API Reference

### Job Management

#### Create Export Job
```http
POST /api/jobs
Content-Type: application/json

{
  "db_type": "postgresql",
  "db_host": "localhost",
  "db_port": 5432,
  "db_username": "user",
  "db_password": "password",
  "db_name": "database",
  "tables": ["public.table1", "public.table2"],
  "compression": "snappy",
  "validate_exports": true
}
```

**Response:**
```json
{
  "job_id": "806cfa9b-6d17-4263-92cc-44b06dd7ee82",
  "status": "queued",
  "message": "Job created successfully"
}
```

#### Get Job Status
```http
GET /api/job/{job_id}
```

**Response:**
```json
{
  "job_id": "806cfa9b-6d17-4263-92cc-44b06dd7ee82",
  "status": "completed",
  "progress": 100,
  "tables_total": 5,
  "tables_completed": 5,
  "start_time": "2025-08-05 17:55:17",
  "end_time": "2025-08-05 17:55:21",
  "duration_seconds": 4.2
}
```

#### Get Job Configuration
```http
GET /api/job/{job_id}/config
```

#### Get Export Details
```http
GET /api/job/{job_id}/export-details
```

### Database Discovery

#### Discover Schemas
```http
POST /api/discover-schemas
Content-Type: application/json

{
  "db_type": "postgresql",
  "db_host": "localhost", 
  "db_port": 5432,
  "db_username": "user",
  "db_password": "password",
  "db_name": "database"
}
```

#### Discover Tables by Schema
```http
POST /api/discover-tables-by-schema
Content-Type: application/json

{
  "db_type": "postgresql",
  "db_host": "localhost",
  "db_port": 5432,
  "db_username": "user", 
  "db_password": "password",
  "db_name": "database",
  "schema": "public"
}
```

#### Get Table Information
```http
POST /api/table-info
Content-Type: application/json

{
  "db_type": "postgresql",
  "db_host": "localhost",
  "db_port": 5432,
  "db_username": "user",
  "db_password": "password", 
  "db_name": "database",
  "table_name": "public.users"
}
```

### Monitoring

#### Get Job History
```http
GET /api/history
```

#### Get Worker Logs
```http
GET /api/logs/worker?lines=100&job_id={job_id}
```

#### Health Check
```http
GET /health
```

---

## 🔍 Troubleshooting

### Common Issues

#### 1. Jobs Remain Queued
**Symptoms:**
- Jobs show "queued" status indefinitely
- No worker log activity

**Diagnosis:**
```bash
# Check if worker process is running
docker exec adu-export python -c "import os; print([p for p in os.listdir('/proc') if p.isdigit()])"

# Check worker logs
docker logs adu-export | grep -i worker
```

**Solutions:**
```bash
# Restart container
docker restart adu-export

# Check worker startup in logs
docker logs adu-export | grep "Starting worker"
```

#### 2. Memory Issues
**Symptoms:**
- Out of memory errors
- Container killed by OOM killer
- Slow chunk processing

**Diagnosis:**
```bash
# Monitor memory usage
docker stats adu-export

# Check chunk size configuration
# Large chunk_size × chunk_workers = high memory usage
```

**Solutions:**
```python
# Reduce chunk size
chunk_size = 500000  # Reduce from 1M to 500K

# Reduce chunk workers
chunk_workers = 4  # Reduce from 8 to 4

# Or increase container memory limit
docker run --memory=16g adu-export:latest
```

#### 3. Database Connection Issues
**Symptoms:**
- Connection timeouts
- Authentication failures
- Pool exhaustion

**Diagnosis:**
```bash
# Test database connectivity
docker exec adu-export python -c "
import psycopg2
conn = psycopg2.connect(
    host='db_host',
    port=5432,
    user='db_user',
    password='db_password',
    database='db_name'
)
print('Connection successful')
"
```

**Solutions:**
```python
# Increase connection timeout
connection_config = {
    'connect_timeout': 60,
    'command_timeout': 3600
}

# Reduce connection pool size
connection_pool_size = 10  # Reduce from 20
```

#### 4. Chunk Processing Failures
**Symptoms:**
- Some chunks fail while others succeed
- Inconsistent export results
- Timeout errors on large chunks

**Diagnosis:**
```bash
# Check for failed chunks in logs
docker logs adu-export | grep -i "chunk.*error"

# Check disk space
df -h /app/exports
```

**Solutions:**
```python
# Implement chunk retry logic
max_retries = 3
retry_delay = 60  # seconds

# Reduce chunk size for problematic tables
problematic_tables = {
    'large_fact_table': {'chunk_size': 100000}
}
```

### Performance Issues

#### Slow Export Performance
**Diagnosis Checklist:**
1. Check CPU utilization: `top` or `htop`
2. Check memory usage: `free -h`
3. Check disk I/O: `iostat -x 1`
4. Check database performance: query execution plans
5. Check network bandwidth: `iftop` or `nethogs`

**Optimization Steps:**
1. **Increase Parallelism:**
   ```python
   table_threads = 16  # Increase from 12
   chunk_workers = 12  # Increase from 8
   ```

2. **Optimize Chunk Size:**
   ```python
   # For fast SSDs and high memory
   chunk_size = 2000000  # 2M rows
   
   # For slower storage or limited memory
   chunk_size = 500000   # 500K rows
   ```

3. **Database-Specific Tuning:**
   ```sql
   -- PostgreSQL/Greenplum
   SET work_mem = '2GB';
   SET shared_buffers = '8GB';
   SET max_parallel_workers_per_gather = 8;
   ```

4. **Storage Optimization:**
   ```bash
   # Use faster compression for speed
   compression = 'snappy'  # Fastest
   
   # Use better compression for space
   compression = 'zstd:3'  # Better ratio
   ```

### Log Analysis

#### Important Log Patterns
```bash
# Worker startup
grep "Starting worker" logs/worker.log

# Job processing
grep "Found queued job" logs/worker.log

# Threading configuration
grep "Processing.*tables using.*threads" logs/worker.log

# Chunk processing
grep "chunk.*parallel" logs/worker.log

# Export completion
grep "Job.*completed successfully" logs/worker.log

# Export organization
grep "Organizing exports" logs/worker.log

# Errors
grep -i error logs/worker.log
```

---

## ⚡ Performance Tuning

### Hardware Optimization

#### CPU Configuration
```bash
# Optimize for 16+ core systems
export OMP_NUM_THREADS=16
export POLARS_MAX_THREADS=16

# Configure thread affinity (Linux)
taskset -c 0-15 python -m adu.worker
```

#### Memory Optimization
```python
# Memory-conscious configuration
memory_config = {
    'chunk_size': 1000000,           # 1M rows per chunk
    'chunk_workers': 8,              # 8 parallel chunks
    'estimated_memory_per_chunk': '1GB',
    'total_estimated_memory': '8GB',  # 8 chunks × 1GB
    'system_memory_buffer': '4GB'     # Leave 4GB for system
}

# For 128GB RAM systems
high_memory_config = {
    'chunk_size': 5000000,    # 5M rows per chunk
    'chunk_workers': 16,      # 16 parallel chunks
    'table_threads': 16       # 16 concurrent tables
}
```

#### Storage Optimization
```bash
# SSD optimization
echo mq-deadline > /sys/block/sda/queue/scheduler
echo 1 > /sys/block/sda/queue/iosched/writes_starved

# Mount options for performance
mount -o noatime,nobarrier /dev/sda1 /app/exports
```

### Database-Specific Tuning

#### PostgreSQL/Greenplum
```sql
-- Connection-level optimizations
SET work_mem = '2GB';
SET maintenance_work_mem = '4GB';
SET max_parallel_workers_per_gather = 8;
SET max_parallel_workers = 16;
SET enable_partitionwise_join = on;
SET enable_partitionwise_aggregate = on;

-- For very large exports
SET temp_buffers = '1GB';
SET effective_cache_size = '64GB';
```

#### Vertica
```sql
-- Session-level optimizations
SET SESSION RESOURCE_POOL = 'export_pool';
SET SESSION LABEL = 'ADU_EXPORT';
SET SESSION UDPARAMETER FOR ExportParameters = 'parallel_workers=16';
```

### Application-Level Tuning

#### Threading Strategy by Dataset Size

**Small Dataset (< 1GB):**
```python
config = {
    'table_threads': 4,
    'chunk_workers': 2,
    'chunk_size': 500000
}
```

**Medium Dataset (1GB - 100GB):**
```python
config = {
    'table_threads': 8,
    'chunk_workers': 4,
    'chunk_size': 1000000
}
```

**Large Dataset (100GB - 1TB):**
```python
config = {
    'table_threads': 12,
    'chunk_workers': 8,
    'chunk_size': 2000000
}
```

**Enterprise Dataset (> 1TB):**
```python
config = {
    'table_threads': 16,
    'chunk_workers': 16,
    'chunk_size': 5000000
}
```

#### Compression Strategy
```python
compression_strategies = {
    'speed_optimized': {
        'compression': 'snappy',
        'level': None,
        'use_case': 'Fast exports, more storage'
    },
    'balanced': {
        'compression': 'zstd',
        'level': 3,
        'use_case': 'Good speed and compression'
    },
    'space_optimized': {
        'compression': 'zstd',
        'level': 9,
        'use_case': 'Maximum compression, slower'
    }
}
```

### Monitoring and Metrics

#### Performance Monitoring
```python
# Export performance metrics
class PerformanceMonitor:
    def __init__(self):
        self.metrics = {
            'rows_per_second': 0,
            'mb_per_second': 0,
            'tables_per_minute': 0,
            'chunks_per_minute': 0,
            'cpu_utilization': 0,
            'memory_utilization': 0,
            'disk_io_utilization': 0
        }
    
    def calculate_throughput(self, rows_exported, duration_seconds):
        return rows_exported / duration_seconds
    
    def log_performance_summary(self, job_id, metrics):
        logger.info(f"Job {job_id} Performance Summary: "
                   f"Rows/sec: {metrics['rows_per_second']:,.0f}, "
                   f"MB/sec: {metrics['mb_per_second']:.1f}, "
                   f"Tables/min: {metrics['tables_per_minute']:.1f}")
```

#### Expected Performance Benchmarks

**16 vCPU + 128GB RAM + SSD:**
- Small tables (< 1M rows): 50,000+ rows/second
- Medium tables (1M-10M rows): 100,000+ rows/second  
- Large tables (> 10M rows): 200,000+ rows/second
- Concurrent table processing: 12+ tables simultaneously
- Total throughput: 2M+ rows/second across all tables

**Network Transfer Rates:**
- 1Gbps network: ~100MB/second theoretical max
- 10Gbps network: ~1GB/second theoretical max
- Local storage: Limited by disk I/O (NVMe SSD: 3-7GB/second)

---

This comprehensive documentation covers all aspects of the ADU Export Application, from basic usage to advanced performance tuning. The application is designed to handle enterprise-scale data exports efficiently while maintaining data integrity and providing excellent visibility into the export process.

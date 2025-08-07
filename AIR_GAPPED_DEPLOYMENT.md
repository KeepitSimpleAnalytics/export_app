# Air-Gapped Environment Deployment Guide

## Overview
This guide provides instructions for deploying the Air-gapped Data Utility (ADU) in an air-gapped environment without Docker Compose, running on port 8504.

## Prerequisites
- Docker installed on the air-gapped system
- The exported Docker image file: `adu-dev-image.tar`

## Step 1: Load the Docker Image

Transfer the `adu-dev-image.tar` file to your air-gapped environment and load it into Docker:

```bash
docker load -i adu-dev-image.tar
```

Verify the image was loaded successfully:
```bash
docker images | grep adu-dev
```

## Step 2: Create Required Directories

Create directories for persistent data storage:

```bash
mkdir -p /opt/adu/data
mkdir -p /opt/adu/logs
mkdir -p /opt/adu/exports
chmod 755 /opt/adu/data /opt/adu/logs /opt/adu/exports
```

## Step 3: Set Environment Variables

Create an environment file for configuration:

```bash
cat > /opt/adu/.env << 'EOF'
SECRET_KEY=your-secret-key-here-change-this-in-production
FERNET_KEY=your-fernet-encryption-key-here-change-this-in-production
ADU_DB_PATH=/app/data/adu.db
FLASK_ENV=production
EOF
```

**Important:** Generate secure keys for production:
- `SECRET_KEY`: Use `python -c "import secrets; print(secrets.token_hex(32))"`
- `FERNET_KEY`: Use `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`

## Step 4: Run the ADU Application Container

Start the unified container with both web interface and background worker:

```bash
docker run -d \
  --name adu-app \
  --env-file /opt/adu/.env \
  -p 8504:5000 \
  -v /opt/adu/data:/app/data \
  -v /opt/adu/logs:/app/logs \
  -v /opt/adu/exports:/app/exports \
  --restart unless-stopped \
  adu-dev:unified
```

**Note:** The container automatically:
- ✅ Initializes the database on startup
- ✅ Starts the Flask web interface on port 8504
- ✅ Starts the background worker process
- ✅ No manual database initialization required

## Step 5: Verify Deployment

Check that the container is running:

```bash
docker ps | grep adu-app
```

Test the web interface:
```bash
curl -s http://localhost:8504/api/history
```

Access the web interface at: `http://localhost:8504`

## Container Management

### View Logs
```bash
# View all logs (web + worker)
docker logs adu-app

# Follow logs in real-time
docker logs -f adu-app

# Filter logs by service
docker logs adu-app 2>&1 | grep "INFO"
```

### Stop Service
```bash
docker stop adu-app
```

### Start Service
```bash
docker start adu-app
```

### Remove Container
```bash
docker rm adu-app
```

## Troubleshooting

### Common Issues

1. **Port 8504 already in use:**
   ```bash
   # Check what's using the port
   netstat -tlnp | grep 8504
   # Use a different port if needed
   docker run ... -p 8505:5000 ...
   ```

2. **Permission issues with mounted volumes:**
   ```bash
   # Fix permissions
   sudo chown -R 1000:1000 /opt/adu/data /opt/adu/logs /opt/adu/exports
   ```

3. **Database connection issues:**
   ```bash
   # Reinitialize database
   docker exec adu-web python init_database.py
   ```

4. **Worker not processing jobs:**
   ```bash
   # Check container logs for worker activity
   docker logs adu-app | grep "Starting worker"
   # Restart container if needed
   docker restart adu-app
   ```

### Health Checks

Verify services are healthy:

```bash
# Check web service
curl -s http://localhost:8504/api/history | jq .

# Check database initialization
docker exec adu-app python -c "from database import get_db_connection; conn = get_db_connection(); cursor = conn.cursor(); cursor.execute('SELECT name FROM sqlite_master WHERE type=\"table\"'); print([r[0] for r in cursor.fetchall()]); conn.close()"

# Check both services are running
docker logs adu-app --tail 10
```

## Security Considerations

1. **Change default keys:** Always generate new `SECRET_KEY` and `FERNET_KEY` values
2. **File permissions:** Ensure data directories have appropriate permissions
3. **Network access:** Consider firewall rules to restrict access to port 8504
4. **Regular updates:** Update the image when security patches are available

## Data Persistence

All application data is stored in the mounted volumes:
- `/opt/adu/data/` - SQLite database
- `/opt/adu/logs/` - Application logs
- `/opt/adu/exports/` - Exported data files

These directories will persist data even if containers are removed and recreated.
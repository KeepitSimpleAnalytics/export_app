# ADU Export Application - Air-gapped Deployment Instructions (Port 8504)

## Generated: Wed Aug  6 11:56:05 PM UTC 2025
## Image File: adu-airgapped-8504-20250806-235507.tar

### Prerequisites
- Docker installed and running
- At least 8GB RAM (16GB+ recommended)
- At least 4 CPU cores (8+ recommended)
- Root/sudo access for directory creation

### Deployment Steps

1. **Transfer the image file to your air-gapped environment:**
   ```bash
   # Copy adu-airgapped-8504-20250806-235507.tar to your target server
   scp adu-airgapped-8504-20250806-235507.tar user@target-server:/path/to/deployment/
   ```

2. **Load the Docker image:**
   ```bash
   docker load -i adu-airgapped-8504-20250806-235507.tar
   ```

3. **Create data directories:**
   ```bash
   sudo mkdir -p /data/adu-export/{exports,database,logs,temp}
   sudo chmod 755 /data/adu-export/{exports,database,logs,temp}
   ```

4. **Run the container:**
   ```bash
   docker run -d \
     --name adu-export-prod \
     --restart unless-stopped \
     -p 8504:8504 \
     -v /data/adu-export/exports:/app/exports \
     -v /data/adu-export/logs:/app/logs \
     -v /data/adu-export/database:/tmp/adu \
     --memory="16g" \
     --cpus="8" \
     adu-export:airgapped-8504
   ```

5. **Verify deployment:**
   ```bash
   # Check container status
   docker ps | grep adu-export-prod
   
   # Check logs
   docker logs adu-export-prod
   
   # Test web interface
   curl http://localhost:8504/
   ```

### Access
- **Web Interface:** http://your-server-ip:8504
- **Container Logs:** `docker logs adu-export-prod`
- **Export Files:** `/data/adu-export/exports/`

### Management Commands

**Start container:**
```bash
docker start adu-export-prod
```

**Stop container:**
```bash
docker stop adu-export-prod
```

**Restart container:**
```bash
docker restart adu-export-prod
```

**View logs:**
```bash
docker logs -f adu-export-prod
```

**Update container (with new image):**
```bash
docker stop adu-export-prod
docker rm adu-export-prod
# Load new image and run step 4 again
```

### Troubleshooting

**Container won't start:**
- Check Docker daemon is running
- Verify port 8504 is not already in use
- Check container logs: `docker logs adu-export-prod`

**Can't access web interface:**
- Verify container is running: `docker ps`
- Check firewall allows port 8504
- Test locally: `curl http://localhost:8504/`

**Performance issues:**
- Increase memory/CPU limits in run command
- Monitor system resources: `htop` or `docker stats`

### Security Notes
- This is designed for air-gapped environments
- No external network access required
- All data stored locally in mounted volumes
- Consider additional firewall rules as needed


# Troubleshooting Guide

## Common Issues

### Container Won't Start

**Symptoms:** `run-container.sh` fails or container stops immediately

**Solutions:**
1. Check if port 8080 is available:
   ```bash
   netstat -tlnp | grep 8080
   ```
2. Try a different port in `run-container.sh`
3. Check Docker logs:
   ```bash
   docker logs adu-export-app
   ```
4. Ensure directories are writable:
   ```bash
   ls -la ../runtime/
   ```

### Application Not Responding

**Symptoms:** Browser shows "connection refused" or timeouts

**Solutions:**
1. Check container status:
   ```bash
   ./status.sh
   ```
2. Verify port mapping:
   ```bash
   docker port adu-export-app
   ```
3. Check application logs:
   ```bash
   docker logs -f adu-export-app
   ```

### Database Connection Issues

**Symptoms:** "Connection failed" errors in web interface

**Solutions:**
1. Verify database server is accessible from container
2. Check firewall rules
3. Ensure database credentials are correct
4. For PostgreSQL/Greenplum: verify `pg_hba.conf` allows connections

### Export Failures

**Symptoms:** Jobs fail or hang during export

**Solutions:**
1. Check available disk space:
   ```bash
   df -h
   ```
2. Monitor memory usage:
   ```bash
   docker stats adu-export-app
   ```
3. Check worker logs in web interface: http://localhost:8080/logs
4. Reduce chunk size for large tables

### Permission Issues

**Symptoms:** "Permission denied" errors

**Solutions:**
1. Ensure Docker daemon is running:
   ```bash
   sudo systemctl status docker
   ```
2. Add user to docker group:
   ```bash
   sudo usermod -aG docker $USER
   ```
3. Check directory permissions:
   ```bash
   ls -la ../runtime/
   ```

## Performance Tuning

### For Large Databases
- Increase container memory limit
- Adjust chunk size in web interface
- Use fewer parallel workers for memory-constrained systems

### For Better Performance
- Use SSD storage for exports directory
- Ensure database server has adequate resources
- Monitor network bandwidth for remote databases

## Logs and Debugging

### Application Logs
```bash
docker logs adu-export-app
```

### Real-time Logs
```bash
docker logs -f adu-export-app
```

### Container Shell Access
```bash
docker exec -it adu-export-app /bin/bash
```

### Check Container Resources
```bash
docker stats adu-export-app
```

## Recovery Procedures

### Reset Application
```bash
./stop-container.sh
./cleanup.sh
./load-image.sh
./run-container.sh
```

### Backup Exports
```bash
tar -czf exports-backup-$(date +%Y%m%d).tar.gz ../runtime/exports/
```

### Restore Exports
```bash
tar -xzf exports-backup-[date].tar.gz -C ../runtime/
```

## Getting Help

1. Check container status: `./status.sh`
2. Review logs: `docker logs adu-export-app`
3. Verify system resources: `df -h && free -h`
4. Check network connectivity to database

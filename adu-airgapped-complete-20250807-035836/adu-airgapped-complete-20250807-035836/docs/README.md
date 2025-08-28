# ADU Export Application - Air-Gapped Deployment

This package contains everything needed to deploy the ADU Export Application in an air-gapped environment without Docker Compose.

## Package Contents

```
adu-airgapped-complete-[timestamp]/
├── adu-export-image.tar          # Docker image file
├── scripts/                      # Management scripts
│   ├── load-image.sh            # Load Docker image
│   ├── run-container.sh         # Start application
│   ├── stop-container.sh        # Stop application
│   ├── status.sh                # Check status
│   └── cleanup.sh               # Remove everything
├── docs/                        # Documentation
│   ├── README.md                # This file
│   └── TROUBLESHOOTING.md       # Common issues
└── runtime/                     # Created automatically
    ├── exports/                 # Exported data
    └── data/                    # Application database
```

## Prerequisites

- Linux system with Docker installed and running
- At least 2GB free disk space
- Network access not required after installation

## Quick Start

1. **Transfer this entire package** to your air-gapped system
2. **Navigate to the scripts directory:**
   ```bash
   cd adu-airgapped-complete-[timestamp]/scripts
   ```
3. **Load the Docker image:**
   ```bash
   ./load-image.sh
   ```
4. **Start the application:**
   ```bash
   ./run-container.sh
   ```
5. **Access the web interface:**
   - Open browser to: http://localhost:8080

## Management Commands

### Start Application
```bash
./run-container.sh
```

### Check Status
```bash
./status.sh
```

### Stop Application
```bash
./stop-container.sh
```

### View Logs
```bash
docker logs adu-export-app
```

### Complete Cleanup
```bash
./cleanup.sh
```

## Application Features

- **Web Interface:** http://localhost:8080
- **Database Export:** Supports PostgreSQL, Greenplum, Vertica
- **Large Table Support:** Automatic chunking and parallel processing
- **Job Management:** Track export progress and history
- **Data Storage:** Exports saved to `../runtime/exports/`

## Configuration

The application runs with these defaults:
- **Port:** 8080
- **Container Name:** adu-export-app
- **Exports Directory:** `../runtime/exports/`
- **Database Directory:** `../runtime/data/`

### Custom Port
To use a different port, edit `run-container.sh` and change:
```bash
HOST_PORT="8080"  # Change to your preferred port
```

### Custom Directories
To use different storage locations, edit `run-container.sh` and change:
```bash
HOST_EXPORTS_DIR="/your/custom/exports/path"
HOST_DATA_DIR="/your/custom/data/path"
```

## Security Notes

- Application is designed for air-gapped environments
- No external network access required
- Database credentials are not stored permanently
- All data remains on your local system

## System Requirements

- **CPU:** 2+ cores recommended
- **Memory:** 4GB+ RAM recommended
- **Storage:** 
  - 1GB for application
  - Additional space for exported data
- **Docker:** Version 20.10+ recommended

## Support

This is a standalone deployment package. Check the troubleshooting guide for common issues.

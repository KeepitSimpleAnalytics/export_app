# ADU Export Application - Import Error Fix Guide

## 🔍 Problem: "attempted relative import with no known package"

**Important:** The Docker image should work identically in both environments. If you're getting import errors in the airgapped environment but not elsewhere, it's likely due to one of these environment differences:

1. **Different Docker run command** - Missing environment variables
2. **Different Docker version** - Behavior changes between versions  
3. **Container startup timing** - Race conditions in airgapped environment
4. **File corruption during transfer** - Image transfer issues

Here's how to diagnose and fix it:

---

## 🛠️ **Fix 1: Verify Image Integrity**

First, make sure the image transferred correctly to your airgapped environment:

### **Check image size and ID:**
```bash
# Compare image details
docker images adu-export:airgapped

# Should show:
# REPOSITORY    TAG        IMAGE ID     CREATED      SIZE
# adu-export    airgapped  15da8233efbf  X hours ago  428MB
```

### **Verify the image loads correctly:**
```bash
# Try to run a simple command inside the image
docker run --rm adu-export:airgapped python --version

# Should output: Python 3.12.x
```

### **If image seems corrupted:**
```bash
# Remove corrupted image
docker rmi adu-export:airgapped

# Re-transfer and load the image
docker load -i adu-export-airgapped-20250805-184113.tar
```

---

## 🛠️ **Fix 2: Exact Docker Run Command**

Use this exact command that matches the working environment:

### **Start with the EXACT command that worked during testing:**
```bash
# Stop any existing container
docker stop adu-export 2>/dev/null || true
docker rm adu-export 2>/dev/null || true

# Use this EXACT command that worked in testing
docker run -d \
  --name adu-export \
  -p 5000:5000 \
  adu-export:airgapped
```

### **If that works, then add volume mounts:**
```bash
# Stop the basic container
docker stop adu-export
docker rm adu-export

# Add volume mounts for persistent data
docker run -d \
  --name adu-export \
  -p 5000:5000 \
  -v /data/adu-export/exports:/app/exports \
  -v /data/adu-export/database:/app/adu/data \
  -v /data/adu-export/logs:/app/logs \
  adu-export:airgapped
```

### **Only if imports still fail, add environment variables:**
```bash
# Last resort - add explicit environment variables
docker run -d \
  --name adu-export \
  -p 5000:5000 \
  -e PYTHONPATH=/app \
  -e FLASK_APP=adu.app \
  -v /data/adu-export/exports:/app/exports \
  -v /data/adu-export/database:/app/adu/data \
  adu-export:airgapped
```

### **Key differences that might cause import errors:**
- **Volume mounts** - These can interfere with the container's internal file structure
- **Missing directories** - If `/data/adu-export/` doesn't exist, volume mounts will fail
- **Environment variable conflicts** - Sometimes explicit env vars cause issues
- **Path permissions** - Volume mount paths need proper permissions

---

## 🛠️ **Fix 3: Docker Version Differences**

Different Docker versions can behave differently:

### **Check Docker version:**
```bash
docker --version
```

### **If using very old Docker (<18.x):**
```bash
# Older Docker versions might need explicit working directory
docker run -d \
  --name adu-export \
  -p 5000:5000 \
  -e PYTHONPATH=/app \
  -e FLASK_APP=adu.app \
  -w /app \
  -v /data/adu-export/exports:/app/exports \
  -v /data/adu-export/database:/app/adu/data \
  adu-export:airgapped
```

---

## 🛠️ **Fix 4: Container Startup Diagnosis**

Let's see exactly what's happening in your airgapped environment:

### **Start container and immediately check logs:**
```bash
# Start container
docker run -d --name adu-export-debug -p 5000:5000 -e PYTHONPATH=/app adu-export:airgapped

# Wait 5 seconds then check logs
sleep 5
docker logs adu-export-debug

# Check if container is still running
docker ps | grep adu-export-debug
```

### **Check the exact error:**
```bash
# Get the full error details
docker logs adu-export-debug 2>&1 | grep -i "import\|error\|traceback" -A 5 -B 5
```

### **Test imports manually:**
```bash
# Test if imports work manually
docker exec adu-export-debug python -c "
import sys
print('Python path:', sys.path)
print('Working directory:', import os; os.getcwd())
try:
    import adu
    print('✅ adu module imports OK')
except Exception as e:
    print('❌ adu import failed:', e)

try:
    from adu import app
    print('✅ adu.app imports OK')
except Exception as e:
    print('❌ adu.app import failed:', e)
"
```

---

## 🛠️ **Fix 5: Emergency Container Access**

If the container keeps failing, access it directly to troubleshoot:

### **Run container in interactive mode:**
```bash
# Run container interactively to debug
docker run -it --rm -e PYTHONPATH=/app adu-export:airgapped /bin/bash

# Once inside, test manually:
cd /app
export PYTHONPATH=/app
export FLASK_APP=adu.app

# Test database init
python init_database.py

# Test worker import
python -c "from adu.worker import poll_for_jobs; print('Worker import OK')"

# Test flask app
python -c "from adu.app import app; print('App import OK')"

# If all work, start normally:
python -m adu.worker &
python -m flask run --host=0.0.0.0 --port=5000
```

---

## 🛠️ **Fix 6: Force Container Rebuild (In Airgapped Environment)**

If the image is definitely corrupted, you can modify it in the airgapped environment:

### **Create a working container and commit changes:**
```bash
# Start a working container
docker run -d --name adu-temp -e PYTHONPATH=/app adu-export:airgapped tail -f /dev/null

# Fix any issues inside
docker exec adu-temp bash -c "
cd /app
# Ensure __init__.py exists
touch adu/__init__.py
# Set proper permissions
chmod +x /app
"

# Commit the fixed container as new image
docker commit adu-temp adu-export:working

# Clean up temp container
docker stop adu-temp
docker rm adu-temp

# Use the working image
docker run -d \
  --name adu-export \
  -p 5000:5000 \
  -e PYTHONPATH=/app \
  -e FLASK_APP=adu.app \
  -v /data/adu-export/exports:/app/exports \
  -v /data/adu-export/database:/app/adu/data \
  adu-export:working
```

---

## 📋 **Diagnosis Steps**

To identify which fix you need, run these diagnostic commands in your airgapped environment:

### **1. Check current PYTHONPATH:**
```bash
docker exec adu-export python -c "import sys; print('PYTHONPATH:', sys.path)"
```

### **2. Check if adu module is importable:**
```bash
docker exec adu-export python -c "import adu; print('ADU module OK')"
```

### **3. Check specific imports:**
```bash
docker exec adu-export python -c "from adu.app import app; print('App import OK')"
docker exec adu-export python -c "from adu.worker import poll_for_jobs; print('Worker import OK')"
```

### **4. Check __init__.py files:**
```bash
docker exec adu-export find /app -name "__init__.py" -type f
```

---

## 🎯 **Most Likely Cause**

**The image should work identically in both environments.** The import error in your airgapped environment is most likely due to:

1. **Missing environment variables** in your `docker run` command
2. **Different volume mount paths** 
3. **Container startup race condition**

### **Use this exact command:**
```bash
docker run -d \
  --name adu-export \
  -p 5000:5000 \
  -e PYTHONPATH=/app \
  -e FLASK_APP=adu.app \
  -e FLASK_DEBUG=False \
  -v /data/adu-export/exports:/app/exports \
  -v /data/adu-export/database:/app/adu/data \
  -v /data/adu-export/logs:/app/logs \
  adu-export:airgapped
```

### **If that doesn't work, run the diagnosis:**
```bash
# Start with debug
docker run -d --name adu-debug -p 5001:5000 -e PYTHONPATH=/app adu-export:airgapped

# Check logs immediately  
docker logs adu-debug

# Test imports manually
docker exec adu-debug python -c "import adu; print('Import OK')"
```

## 🚀 **Expected Results After Fix**

Once fixed, you should see:
- Container starts without import errors
- Web interface accessible at `http://localhost:5000`
- Worker process running and polling for jobs
- Both Flask app and worker processes active

The **PYTHONPATH environment variable** is the most common solution for this type of import error in containerized Python applications.

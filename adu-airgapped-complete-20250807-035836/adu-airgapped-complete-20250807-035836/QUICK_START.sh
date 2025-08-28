#!/bin/bash

echo "🚀 ADU Export Application - Quick Start"
echo "======================================"
echo ""
echo "This will load and start the ADU Export Application"
echo ""

# Navigate to scripts directory
cd scripts

# Load image
echo "1️⃣  Loading Docker image..."
./load-image.sh

if [ $? -eq 0 ]; then
    echo ""
    echo "2️⃣  Starting application..."
    ./run-container.sh
    
    if [ $? -eq 0 ]; then
        echo ""
        echo "🎉 SUCCESS! Application is running"
        echo ""
        echo "🌐 Open your browser to: http://localhost:8080"
        echo "📖 Read the documentation in: docs/README.md"
        echo "🔧 Use './scripts/status.sh' to check application status"
    fi
else
    echo "❌ Failed to load image. Check docs/TROUBLESHOOTING.md"
fi

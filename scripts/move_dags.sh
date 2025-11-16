#!/bin/bash
# Script to move DAGs from temp_dags to dags directory
# Run with: bash scripts/move_dags.sh

echo "Moving Lakehouse DAGs to dags/ directory..."

# Check if temp_dags exists
if [ ! -d "temp_dags" ]; then
    echo "Error: temp_dags directory not found"
    exit 1
fi

# Copy files (will prompt for sudo if needed)
if cp temp_dags/lakehouse_*.py dags/ 2>/dev/null; then
    echo "✓ Successfully copied DAGs to dags/"
    echo "Files copied:"
    ls -1 dags/lakehouse_*.py
else
    echo "Permission denied. Trying with sudo..."
    sudo cp temp_dags/lakehouse_*.py dags/
    sudo chown $USER:$USER dags/lakehouse_*.py
    echo "✓ Successfully copied DAGs to dags/ with sudo"
fi

echo ""
echo "DAGs are now in dags/ directory and ready to use!"


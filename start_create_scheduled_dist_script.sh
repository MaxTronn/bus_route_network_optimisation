#!/bin/bash

# python3 /home/dev/apps/vehicle-performance-monitoring/create_scheduled_dist_db.py
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd "$SCRIPT_DIR" || exit
echo "current wd: $SCRIPT_DIR"

./venv/bin/python -m create_scheduled_dist_db
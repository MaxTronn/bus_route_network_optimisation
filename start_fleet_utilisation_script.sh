#!/bin/bash

# python3 /home/dev/apps/vehicle-performance-monitoring/fleet_utilisation.py
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd "$SCRIPT_DIR" || exit
echo "current wd: $SCRIPT_DIR"

./venv/bin/python -m fleet_utilisation

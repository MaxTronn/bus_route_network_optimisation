SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd "$SCRIPT_DIR" || exit
echo "current wd: $SCRIPT_DIR"

./venv/bin/python -m DistanceSpeed_realtime

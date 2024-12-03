#!/usr/bin/env bash

echo "Starting the LEAF program"
# Starting the LEAF program
ls /app
# In the future, this will be part of the docker image
if [ -f /app/requirements.txt ]; then
    echo "Requirements file found"
    cat /app/requirements.txt
    pip install --no-cache-dir --force-reinstall -r /app/requirements.txt
else
    echo "No requirements.txt file found"
fi

leaf --config /app/config.yaml
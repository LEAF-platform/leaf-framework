#!/bin/bash

# Start Mosquitto
mosquitto -d

# Start Redis
redis-server --daemonize yes

# Execute any passed command (fallback to CMD or user-provided command)
exec "$@"


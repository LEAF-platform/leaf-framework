#!/bin/bash

echo "#############################################"
echo "Welcome to the LEAF backend setup script."
echo "This script will start all the services required for the backend."
echo "#############################################"
echo ""
echo ""

# Load the .env file
set -o allexport
source .env
set +o allexport

# Obtain local directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Go to the directory
cd $DIR

# Start the YAML file
docker-compose -f "$DIR/docker-compose.yml" up -d --remove-orphans

# Function to check if the container is running
wait_for_container() {
    echo "Waiting for EMQX container to start..."
    while ! docker ps --format '{{.Names}}' | grep -q "^emqx$"; do
        sleep 2
    done
    echo "EMQX container detected, waiting for full initialization..."
    sleep 10  # Give extra time for full boot
}

# Function to check EMQX cluster status
check_cluster_status() {
    echo "Checking EMQX cluster status..."
    docker exec emqx sh -c "emqx ctl cluster status"
}

# Function to initialize admin accounts
init_accounts() {
    echo "Creating dashboard account..."
    output=$(docker exec emqx sh -c "emqx ctl admins add \"$EMQX_DASHBOARD_USER\" \"$EMQX_DASHBOARD_PASSWORD\"")
    if echo "$output" | grep -q "username_already_exists"; then
        echo "Dashboard account \"$EMQX_DASHBOARD_USER\" already exists."
        echo "To reset the password, run the following command:"
        echo "docker exec emqx sh -c \"emqx ctl admins passwd $EMQX_DASHBOARD_USER YOUR_NEW_PASSWORD\""
    else
        echo "Dashboard account created."
        echo "Dashboard User: $EMQX_DASHBOARD_USER"
        echo "Dashboard Password: $EMQX_DASHBOARD_PASSWORD"
    fi
}

# Run the functions
wait_for_container
check_cluster_status
init_accounts

echo ""
echo ""
echo "############################################"
echo "All services started successfully."

# Write out how to access all the different applications
HOST_NAME=$(hostname)

# EMQX
echo "EMQX Dashboard: http://$HOST_NAME:18083"

# Node Red
echo "Node-RED: http://$HOST_NAME:1880"

# Grafana
echo "Grafana: http://$HOST_NAME:3000"

# PGAdmin
echo "PGAdmin: http://$HOST_NAME:5050"
echo "############################################"
# Use the official Python 3.12 image as the base image
# FROM python:3.12
FROM ubuntu:jammy-20240911.1

# Set the working directory in the container
# WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

USER root

# Upgrade pip and install dependencies
RUN apt-get update && apt-get install --yes python3 python3-pip python3-venv && \
    python3 -m venv /app/venv && \
    /app/venv/bin/pip install --upgrade pip && \
    /app/venv/bin/pip install -r requirements.txt && \
    /app/venv/bin/pip install pytest pytest-xdist

# Update PATH to use the venv
ENV PATH="/app/venv/bin:$PATH"

RUN apt-get update && apt-get install -y lsb-release wget && apt-get clean all

# Install KeyDB
RUN echo "deb https://download.keydb.dev/open-source-dist $(lsb_release -sc) main" | tee /etc/apt/sources.list.d/keydb.list && \
    wget -O /etc/apt/trusted.gpg.d/keydb.gpg https://download.keydb.dev/open-source-dist/keyring.gpg && apt update && apt install --yes systemctl keydb-server=6:6.3.3-1~jammy1 keydb-tools=6:6.3.3-1~jammy1


# TODO install MQTT broker (mosquitto?)
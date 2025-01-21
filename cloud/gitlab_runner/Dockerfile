# Use the official Python 3.12 image as the base image
# FROM python:3.12
FROM ubuntu:noble

# Upgrade pip and install dependencies
RUN apt-get update && \
    apt-get install -y python3-venv python3-pip lsb-release wget python3-launchpadlib software-properties-common &&\
    apt-get update && \
    add-apt-repository -y ppa:mosquitto-dev/mosquitto-ppa && \
    apt-get install --yes mosquitto mosquitto-clients redis-server && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Add pyproject.toml and poetry.lock to the container \
COPY pyproject.toml ./

RUN python3 -m venv /venv && \
    . /venv/bin/activate && \
    pip install poetry && \
    poetry config virtualenvs.create false && \
    poetry install

# Activate the virtual environment by default
ENV PATH="/venv/bin:$PATH"

# Expose ports (e.g., Mosquitto and Redis)
EXPOSE 1883 6379

# Copy the entrypoint script
COPY entrypoint.sh /entrypoint.sh

# Set the entrypoint script as the entrypoint
RUN chmod +x /entrypoint.sh

# Use entrypoint script
ENTRYPOINT ["/entrypoint.sh"]
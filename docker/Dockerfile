FROM python:3.12

WORKDIR /app

# Copy the start script to the container
ADD start.sh /app/start.sh

RUN pip install --upgrade pip && \
    pip install git+https://gitlab.com/LabEquipmentAdapterFramework/leaf.git@dev

# Install all adapters
RUN pip install git+https://gitlab.com/LabEquipmentAdapterFramework/leaf-adapters/leaf-hello-world
#RUN pip install git+https://gitlab.com/LabEquipmentAdapterFramework/leaf-adapters/leaf-irods
#RUN pip install git+https://gitlab.com/LabEquipmentAdapterFramework/leaf-adapters/leaf-minknow
#RUN pip install git+https://gitlab.com/LabEquipmentAdapterFramework/leaf-adapters/leaf-maq

# Install keydb
RUN echo "deb https://download.keydb.dev/open-source-dist $(lsb_release -sc) main" | tee /etc/apt/sources.list.d/keydb.list && \
    wget -O /etc/apt/trusted.gpg.d/keydb.gpg https://download.keydb.dev/open-source-dist/keyring.gpg && \
    apt update && apt install keydb

# Start the application
ENTRYPOINT ["bash", "/app/start.sh"]
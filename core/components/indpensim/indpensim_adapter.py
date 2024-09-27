import logging
import asyncio
import json
from datetime import datetime, timedelta

from core.mqtt_client import MQTTClient

# Set the logging level
logging.basicConfig(level=logging.INFO)

# Define a global variable to hold the data
global_data: dict[str, str] = {}
global_start_time: datetime = datetime.now()

# Function to prepare the data
def prepare_data(data: dict[str, str]) -> dict[str, str]:
    try:
        time_h = float(data['Time (h)'])
        time = get_global_start_time() + timedelta(hours=time_h)
        data['time'] = time
        logging.info("Preparing data: %s", data)
        return data
    except Exception as e:
        logging.error(f"Error in prepare_data: {e}")

async def main() -> None:
    logging.info("Starting the IndPenSim simulation program")
    # Monitor the global_data variable if it changes
    while True:
        data = get_global_data()
        if data:
            logging.info("Data received")
            data = prepare_data(data)
            # Send message to the MQTT broker
            logging.info("Sending data to MQTT broker")
            logging.info("Data: %s", data)
            MQTTClient().publish("indpensim/data", json.dumps(data))
            set_global_data(None)
        else:
            logging.info("No data received yet")
        # Sleep for a while
        await asyncio.sleep(0.1)

# Function to set the global data
def set_global_data(data: dict[str, str]) -> None:
    global global_data
    global_data = data

# Function to get the global data
def get_global_data() -> dict[str, str]:
    global global_data
    return global_data

# Function to set the global start time
def set_global_start_time(start_time: datetime) -> None:
    global global_start_time
    global_start_time = start_time

# Function to get the global start time
def get_global_start_time() -> datetime:
    global global_start_time
    return global_start_time

import asyncio
import json
import logging
import uuid
from datetime import datetime, timedelta

from influxobject import InfluxPoint

from core.start import get_keydb_client

# Set the logging level
logging.basicConfig(level=logging.INFO)

# Define a global variable to hold the data
global_data_lists: list[dict[str, str]] = []
global_start_time: datetime = datetime.now()

# Function to prepare the data
def prepare_data(data: dict[str, str]) -> InfluxPoint:
    try:
        data = get_global_data()
        influx_point = InfluxPoint()
        influx_point.set_measurement('indpensim')
        influx_point.set_fields(data)

        time_h = float(data['Time (h)'])
        time = get_global_start_time() + timedelta(hours=time_h)
        influx_point.set_timestamp(time)
        influx_point.add_tag('topic', 'leaf-test/indpensim')
        logging.info("Preparing data: %s", data)
        return influx_point
    except Exception as e:
        logging.error(f"Error in prepare_data: {e}")

async def main() -> None:
    logging.info("Starting the IndPenSim simulation program")
    # Monitor the global_data variable if it changes
    keydb_client = get_keydb_client()
    size: int = await keydb_client.client.dbsize()
    logging.info(f"KeyDB client: {keydb_client} of size {size}")
    while True:
        raw_data = get_global_data()
        logging.debug(f"Raw data: {raw_data}")
        if raw_data:
            logging.info("Data received")
            influx_point = prepare_data(raw_data)
            # Send message to the MQTT broker
            logging.info("Data: %s", influx_point)
            # Calculate hash of the data
            # hash_data = hash(json.dumps(data))
            hash_data = str(uuid.uuid4())
            await keydb_client.client.set(hash_data, json.dumps(influx_point.to_json(), indent=4, sort_keys=True))
            num_keys = await keydb_client.client.dbsize()
            logging.info(f"Data sent to KeyDB now with a size of {num_keys} entries")
            set_global_data(None)
        else:
            logging.debug("No data received yet")
        # Sleep for a while
        await asyncio.sleep(0.1)

# Function to set the global data
def set_global_data(data: dict[str, str]) -> None:
    global global_data_lists
    global_data_lists.append(data)
    logging.info(f"Contains {len(global_data_lists)} indpensim data sets")

# Function to get the size of the global data
def get_size_global_data() -> int:
    global global_data_lists
    return len(global_data_lists)

# Function to get the global data
def get_global_data() -> dict[str, str]:
    global global_data_lists
    return global_data_lists.pop() if global_data_lists else None

# Function to set the global start time
def set_global_start_time(start_time: datetime) -> None:
    global global_start_time
    global_start_time = start_time

# Function to get the global start time
def get_global_start_time() -> datetime:
    global global_start_time
    return global_start_time

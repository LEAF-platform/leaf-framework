import unittest
import os
import gzip
import json
import asyncio
from core.components.indpensim.indpensim_adapter import main, set_global_data, get_global_data, set_global_start_time, get_global_start_time
import logging
from datetime import datetime

# Set the logging level
logging.basicConfig(level=logging.INFO)

class TestIndPenSim(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        # Initialize the MQTT client
        init_mqtt()
        # Set the global start time
        set_global_start_time(datetime.strptime("2024-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"))
        # Start main() in the background
        logging.info("Starting the main program")
        self.main_task = asyncio.create_task(main())
        logging.info("Main program started")

    async def asyncTearDown(self) -> None:
        # Cancel the main program task after test finishes
        logging.info("Stopping the main program")
        self.main_task.cancel()
        try:
            await self.main_task
        except asyncio.CancelledError:
            logging.info("Main program task cancelled")

    async def test_process_data(self) -> None:
        logging.info("Testing the processing of data")
        # Give the main program some time to run
        await asyncio.sleep(2)  # Adjust this as necessary
        # Print current directory
        logging.info(f"Current directory: {os.getcwd()}")
        # List all files in the data directory
        data_dir = "data"
        files = os.listdir(data_dir)
        self.assertGreater(len(files), 0)
        
        # Only accept .csv.gz files
        for file in files:
            if file != 'IndPenSim_V3_Batch_1_top10.csv.gz':
                continue
            if not file.endswith(".csv.gz"):
                logging.info(f"Skipping file: {file}")
                continue
            
            # Read the file
            with gzip.open(os.path.join(data_dir, file), "r") as f:
                for index, lineb in enumerate(f):
                    line = lineb.decode("utf-8")
                    if index == 0:
                        header = line.strip()
                    else:
                        # Make a dictionary from the header and line
                        data = dict(zip(header.split(","), line.strip().split(",")))
                        
                        # Remove all keys that are numbers
                        for key in list(data.keys()):
                            if key.isdigit():
                                del data[key]
                        
                        # Check if the dictionary is not empty
                        self.assertGreater(len(data), 0)
                        
                        # Turn it into a JSON object
                        content = json.dumps(data)
                        
                        # Check if the content is not empty
                        self.assertGreater(len(content), 0)
                        
                        # Check if the content is valid JSON
                        try:
                            json_object = json.loads(content)
                            for key, value in json_object.items():
                                # Check if it can be converted to a float
                                try:
                                    json_object[key] = float(value)
                                    # If integer, convert to int
                                    if json_object[key].is_integer():
                                        json_object[key] = int(json_object[key])
                                except ValueError:
                                    pass
                            # Send the valid JSON to a global variable in the main program
                            set_global_data(json_object)
                            logging.info("Data sent to the main program")
                            
                            # Allow a brief pause to simulate the processing delay
                            await asyncio.sleep(1)  # Use async sleep
                            
                        except json.JSONDecodeError:
                            self.fail("Invalid JSON content")


def init_mqtt():
        from core.mqtt_client import MQTTClient
        
        # Initialize the MQTT client
        mqtt_client = MQTTClient(broker_host='test.mosquitto.org', broker_port=1883, client_id='MyMQTTClient')

        # Connect to the MQTT broker
        mqtt_client.connect()

        # Subscribe to a topic (for receiving messages)
        mqtt_client.subscribe("leaf/component/indpensim")

        # Publish a message to a topic (for sending messages)
        mqtt_client.publish("leaf/component/indpensim", "Sensor data here")
    
if __name__ == '__main__':
    unittest.main()
    init_mqtt()
    set_global_start_time(datetime.strptime("2024-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"))
    

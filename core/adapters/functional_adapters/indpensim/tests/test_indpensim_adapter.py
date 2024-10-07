import asyncio
import gzip
import json
import logging
import os
import unittest
from datetime import datetime
from time import sleep

import core.start as core
from core.components.indpensim.indpensim_adapter import main, set_global_data, set_global_start_time, get_global_data, \
    get_size_global_data

# Set the logging level
logging.basicConfig(level=logging.INFO)

class TestIndPenSim(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        # Initialize the program
        logging.info("Initializing the program")
        asyncio.create_task(core.main('../../../config.ini'))
        logging.info("Program initialized")
        # Set the global start time
        set_global_start_time(datetime.strptime("2024-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"))
        # Start the main program after a few seconds
        while core.get_keydb_client() is None:
            logging.info("Waiting for KeyDB client to be initialized")
            await asyncio.sleep(1)
        logging.info(f"KeyDB client initialized of size {core.get_keydb_client().client.dbsize()}")
        logging.info("Starting the main program")
        asyncio.create_task(main())

    async def asyncTearDown(self) -> None:
        # Check global data
        while get_size_global_data() > 0:
            logging.info("Waiting for global data to be processed")
            await asyncio.sleep(1)


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
            # if file != 'IndPenSim_V3_Batch_1_top10.csv.gz':
            #     continue
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
                        except json.JSONDecodeError:
                            self.fail("Invalid JSON content")

if __name__ == '__main__':
    unittest.main()
    set_global_start_time(datetime.strptime("2024-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"))
    

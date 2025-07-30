import json
import logging
import os
import threading
import time
import unittest
import redis
import yaml

import leaf.start
from leaf.utility.logger.logger_utils import get_logger

logger = get_logger(__name__, log_file="input_module.log", log_level=logging.DEBUG)

class TestKeyDB(unittest.TestCase):

    def setUp(self):
        # Connect to Redis (localhost:6379 by default)
        self.db = redis.Redis(host='localhost', port=6379, db=0)

    def tearDown(self):
        # Clean up Redis after each test
        self.db.flushdb()

    def test_set_and_get(self):
        # Set a key-value pair and verify that it can be retrieved
        self.db.set('key', 'value')
        result = self.db.get('key').decode('utf-8')  # Decode from bytes to string
        self.assertEqual(result, 'value')

    def test_delete(self):
        # Set a key-value pair and then delete it
        self.db.set('key', 'value')
        self.db.delete('key')
        result = self.db.get('key')
        self.assertIsNone(result)  # Redis returns None if the key doesn't exist

    def test_flushdb(self):
        # Set multiple key-value pairs and flush the database
        self.db.set('key1', 'value1')
        self.db.set('key2', 'value2')
        self.db.flushdb()
        
        result1 = self.db.get('key1')
        result2 = self.db.get('key2')
        
        self.assertIsNone(result1)  # Both keys should be deleted
        self.assertIsNone(result2)

    def test_leaf(self):
        config = "../../../tests/static_files/test_config_keydb.yaml"
        # Load configuration from the YAML file
        if not os.path.exists(config):
            raise FileNotFoundError(f"Configuration file {config} not found.")
        with open(config, 'r') as file:
            config_data = yaml.safe_load(file)
            print("Configuration loaded:", config_data)

        # Get KEYDB output module configuration
        keydb_config = config_data['OUTPUTS'][0]
        print("KeyDB configuration:", keydb_config)
        host = keydb_config.get('host')
        port = keydb_config.get('port')
        db = keydb_config.get('db')

        print(f"Connecting to KeyDB at {host}:{port}, DB: {db}")
        # Connect to the KeyDB instance
        try:
            keydb_client = redis.Redis(host=host, port=port, db=db)
            print("Connected to KeyDB successfully")
        except redis.ConnectionError as e:
            print("Failed to connect to KeyDB:", e)
            self.fail("Could not connect to KeyDB")

        def run_leaf():
            try:
                print("Launching LEAF")
                leaf.start.main(["--nogui", "--config", config])
                print("LEAF exited normally")
            except Exception as e:
                print("LEAF crashed:", e)
        # Start the leaf application in a separate thread
        thread = threading.Thread(target=run_leaf, daemon=True)
        thread.start()
        while True:
            try:
                # Check if the thread is still alive
                if not thread.is_alive():
                    print("LEAF thread has finished")
                    break
                # Obtain data from KeyDB
                keys = keydb_client.keys()
                if len(keys) == 2:
                    assert keys[0] == b'example_hello_world_institute1/HelloWorld/example_hello_world_id1/experiment/undefined/measurement/bioreactor_example'
                    assert keys[1] == b'example_hello_world_institute1/HelloWorld/example_hello_world_id1/details'
                    logger.info("Current keys in KeyDB: %s", keys)
                    # Number of values in the list
                    size_before = keydb_client.llen(keys[0])
                    logger.info("Number of values in the list: %d", size_before)
                    # Obtain the values for the keys
                    value1 = json.loads(keydb_client.lpop(keys[0]))
                    logger.info("Value for key1: %s", value1)
                    # Number of values in the list
                    size_after = keydb_client.llen(keys[0])
                    logger.info("Number of values in the list: %d", size_after)
                    assert size_after == size_before - 1
                    break
                # Sleep for a while to avoid busy waiting
                time.sleep(1)
            except KeyboardInterrupt:
                print("Interrupted by user, stopping LEAF")
                break
        # Stop the leaf thread
        thread.join(1)


    def test_keydb_switch_to_mqtt(self):
        config = "../../../tests/static_files/test_config_keydb.yaml"
        # Start the LEAF application with KeyDB output module
        logger.info("Starting LEAF with KeyDB output module")
        def run_leaf():
            try:
                logger.info("Launching LEAF")
                leaf.start.main(["--nogui", "--config", config])
                logger.info("LEAF exited normally")
            except Exception as e:
                logger.info("LEAF crashed:", e)
        # Start the leaf application in a separate thread
        thread = threading.Thread(target=run_leaf, daemon=True)
        thread.start()

        # Load configuration from the YAML file
        if not os.path.exists(config):
            raise FileNotFoundError(f"Configuration file {config} not found.")
        with open(config, 'r') as file:
            config_data = yaml.safe_load(file)
            logger.info("Configuration loaded:", config_data)

        # Get KEYDB output module configuration
        keydb_config = config_data['OUTPUTS'][0]
        logger.info("KeyDB configuration:", keydb_config)
        host = keydb_config.get('host')
        port = keydb_config.get('port')
        db = keydb_config.get('db')

        logger.info(f"Connecting to KeyDB at {host}:{port}, DB: {db}")
        # Connect to the KeyDB instance
        try:
            keydb_client = redis.Redis(host=host, port=port, db=db)
            # Delete all keys in KeyDB to start fresh
            keydb_client.flushdb()
            logger.info("Flushed KeyDB database")
            # Wait for the LEAF thread to start and populate KeyDB
            while True:
                # Check if the thread is still alive
                if not thread.is_alive():
                    logger.info("LEAF thread has finished")
                    break
                # Obtain data from KeyDB
                keys = keydb_client.keys()
                if len(keys) == 2:
                    assert keys[0] == b'example_hello_world_institute1/HelloWorld/example_hello_world_id1/experiment/undefined/measurement/bioreactor_example'
                    assert keys[1] == b'example_hello_world_institute1/HelloWorld/example_hello_world_id1/details'
                    logger.info("Current keys in KeyDB: %s", keys)
                    # Number of values in the list
                    size_now = keydb_client.llen(keys[0])
                    logger.info("Number of values in the list: %d", size_now)
                    if size_now > 10:
                        # Stop the LEAF thread
                        logger.info("Stopping LEAF thread due to sufficient data in KeyDB")
                        thread.join(1)
                        break
                # Sleep for a while to avoid busy waiting
                time.sleep(1)
            logger.info("Connected to KeyDB successfully")
        except redis.ConnectionError as e:
            logger.info("Failed to connect to KeyDB:", e)
            self.fail("Could not connect to KeyDB")

        # Start LEAF in MQTT mode
        logger.info("Switching LEAF to MQTT mode")
        config = "../../../tests/static_files/test_config_mqtt_with_keydb_fallback.yaml"
        thread = threading.Thread(target=run_leaf, daemon=True)
        thread.start()
        # Wait for the thread to finish
        while True:
            time.sleep(1)


if __name__ == '__main__':
    unittest.main()

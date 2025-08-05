import json
import logging
import multiprocessing
import os
import threading
import time
import unittest
import redis
import yaml

import leaf.start
from leaf.modules.output_modules.mqtt import MQTT
from leaf.utility.logger.logger_utils import get_logger

curr_dir: str = os.path.dirname(os.path.realpath(__file__))
logger = get_logger(__name__, log_file="input_module.log", log_level=logging.DEBUG)

def run_leaf(config):
    logger.info("Starting LEAF process")
    leaf.start.main(["--nogui", "--config", config])

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

        config = os.path.join(os.path.dirname(__file__), "..","..","..","tests", "static_files", "test_config_keydb.yaml")
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
                keys = keydb_client.keys("keydb_test_instance*")
                print("Current keys in KeyDB:", keys)
                if len(keys) == 2:
                    assert keys[0] == b'example_hello_world_institute1/HelloWorld/keydb_test_instance/experiment/undefined/measurement/bioreactor_example'
                    assert keys[1] == b'example_hello_world_institute1/HelloWorld/keydb_test_instance/details'
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


    # To make this test work you need to disable the ....
    #     signal.signal(signal.SIGINT, signal_handler)
    #     signal.signal(signal.SIGTERM, signal_handler)
    #     sys.excepthook = handle_exception
    # for now...
    @unittest.skip("Skipping test_keydb_switch_to_mqtt due to signal handling issues")
    def test_keydb_switch_to_mqtt(self):
        config = os.path.join(os.path.dirname(__file__), "..","..","..","tests", "static_files", "test_config_keydb.yaml")

        if not os.path.exists(config):
            raise FileNotFoundError(f"Configuration file {config} not found.")

        # Load configuration from the YAML file
        with open(config, 'r') as file:
            config_data = yaml.safe_load(file)
            logger.info("Configuration loaded:", config_data)

        # Get KEYDB output module configuration
        keydb_config = config_data['OUTPUTS'][0]
        logger.info("KeyDB configuration:", keydb_config)
        host = keydb_config.get('host')
        port = keydb_config.get('port')
        db = keydb_config.get('db')

        keydb_client = redis.Redis(host=host, port=port, db=db)
        # Delete all keys in KeyDB to start fresh
        keydb_client.flushdb()
        logger.info("Deleted all keys in KeyDB")

        # Start the LEAF application with KeyDB output module
        logger.info("Starting LEAF with KeyDB output module")
        # Start the leaf application in a separate thread
        p = multiprocessing.Process(target=run_leaf, args=(config,))
        p.start()

        logger.info(f"Connecting to KeyDB at {host}:{port}, DB: {db}")
        # Connect to the KeyDB instance
        keydb_client = redis.Redis(host=host, port=port, db=db)
        # Wait for the LEAF thread to start and populate KeyDB
        while True:
            # Check if the thread is still alive
            if not p.is_alive():
                logger.info("LEAF thread has finished")
                break
            # Obtain data from KeyDB
            keys = keydb_client.keys()
            while True:
                logger.info("Current keys in KeyDB: %s", keys)
                if b'example_hello_world_institute1/HelloWorld/example_hello_world_id1/experiment/undefined/measurement/bioreactor_example' in keys and b'example_hello_world_institute1/HelloWorld/example_hello_world_id1/details' in keys:
                    logger.info("Current keys in KeyDB: %s", keys)
                    # Number of values in the list
                    size_now = keydb_client.llen(keys[0])
                    logger.info("Number of values in the list: %d", size_now)
                    if size_now > 5:
                        # Stop the LEAF thread
                        logger.info("Stopping LEAF thread due to sufficient data in KeyDB")
                        p.join(1)
                        p.terminate()
                        break
                # Sleep for a while to avoid busy waiting
                time.sleep(1)

        # Check if the LEAF process is still alive by counting the number of messages in KeyDB
        while p.is_alive():
            logger.info("Number of messages in KeyDB: %d", keydb_client.llen(keys[0]))
            p.terminate()
            p.join(1)

        # Obtain all messages from KeyDB to check if they are sent over MQTT
        keys = keydb_client.keys()
        message_store = {}
        for key in keys:
            key = key.decode('utf-8')  # Decode bytes to string
            message_store[key] = set()
            logger.info("Key: %s", key)
            messages = keydb_client.lrange(key, 0, -1)
            for message in messages:
                logger.info("Message: %s", message.decode('utf-8'))
                message_store[key].add(json.loads(message.decode('utf-8')))

        logger.info("Message store: %s", message_store)

        # Start LEAF in MQTT mode
        logger.info("Switching LEAF to MQTT mode")
        config = os.path.join(os.path.dirname(__file__), "..", "..", "..", "tests", "static_files",
                              "test_config_mqtt_with_keydb_fallback.yaml")

        def on_message_test(client, userdata, msg):
            """Callback function to handle incoming MQTT messages."""
            logger.info(f"Received message on topic {msg.topic}: {msg.payload.decode('utf-8')}")
            keys = message_store.keys()
            if  msg.topic in keys:
                for m in message_store[msg.topic]:
                    if json.loads(msg.payload.decode('utf-8')) == m:
                        logger.info("Message matches stored message, test passed")
                        # Delete the message from message_store
                        message_store[msg.topic].remove(m)
                        logger.info(f"Number of remaining messages: {len(message_store[msg.topic])}")
                        if len(message_store[msg.topic]) == 0:
                            logger.info("All messages for topic %s have been received", msg.topic)
                            del message_store[msg.topic]
                        break

        output = MQTT("localhost", 1883)
        # output.client.connect("localhost", 1883, 60)
        output.client.subscribe("#")
        output.client.on_message = on_message_test

        # run_leaf(config)

        p = multiprocessing.Process(target=run_leaf, args=(config,))
        p.start()

        # Wait for the thread to finish
        while p.is_alive():
            logger.info(f"Message store: {message_store}")
            if len(message_store) == 0:
                logger.info("All messages have been received, stopping LEAF")
                p.terminate()
                p.join(1)
                break
            time.sleep(1)



if __name__ == '__main__':
    unittest.main()

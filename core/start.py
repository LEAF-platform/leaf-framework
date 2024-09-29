import sys
import asyncio

import core.keydb_client
from .config_loader import Config  # Assuming your Config class is in config_loader.py
from .mqtt_client import MQTTClient  # Assuming you have an MqttClient class
from .keydb_client import AsyncKeyDBClient  # Assuming you have an AsyncKeyDBClient class
import os
import logging

logging.basicConfig(level=logging.DEBUG)

# Global variable to hold the KeyDB client
keydb_client: AsyncKeyDBClient = None

async def main(config_file):
    # Get absolute path of the config file
    config_file = os.path.abspath(config_file)
    logging.info(f"Starting with config file: {config_file}")
    if not os.path.exists(config_file):
        logging.error(f"Config file not found: {config_file}")
        sys.exit(1)
    # 1. Load the configuration
    config = Config(config_file)

    # Print dict
    print(config.to_dict())

    # 2. Initialize the MQTT client
    mqtt_broker = config.get('mqtt', 'broker')
    mqtt_port = config.get_int('mqtt', 'port')
    mqtt_username = config.get('mqtt', 'username')
    mqtt_password = config.get('mqtt', 'password')
    mqtt_clientid = config.get('mqtt', 'clientid')

    mqtt_client = MQTTClient(broker_host=mqtt_broker, broker_port=mqtt_port, broker_username=mqtt_username, broker_password=mqtt_password, client_id=mqtt_clientid)

    # 3. Initialize KeyDB client
    keydb_host = config.get('keydb', 'host')
    keydb_port = config.get_int('keydb', 'port')
    keydb_db = config.get_int('keydb', 'db')

    # 4. Start the MQTT client and KeyDB
    try:
        # Connect to the MQTT broker
        mqtt_client.connect()  # Assuming this is synchronous
        print("MQTT client connected")
        # Create an instance of the KeyDB client
        global keydb_client
        keydb_client = AsyncKeyDBClient(host=keydb_host, port=keydb_port, db=keydb_db)
        # Connect to KeyDB
        await keydb_client.connect()
        logging.info("KeyDB client connected")
        await keydb_client.run_forever(mqtt_client)
    except KeyboardInterrupt:
        # Handle shutdown gracefully
        print("Shutting down...")
        mqtt_client.disconnect()
        await keydb_client.close()  # Close KeyDB connection asynchronously

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

def get_keydb_client():
    # while keydb_client is None:
    logging.debug("KeyDB client not initialized.")
    return keydb_client

if __name__ == "__main__":
    # Program starts here, passing the configuration file as an argument
    if len(sys.argv) != 2:
        # Check if a configuration file exists
        print(f"Current path is: {os.getcwd()}")
        if not os.path.exists('config.ini'):
            print("No configuration file found. Please create a 'config.ini' file.")
            # TODO generate or copy a default config file
            sys.exit(1)
        sys.argv.append('config.ini')
        print("To use a different configuration file, pass it as an argument.")

    # Run the asynchronous main function using asyncio's event loop
    asyncio.run(main(sys.argv[1]))

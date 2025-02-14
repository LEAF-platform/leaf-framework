import multiprocessing
import json
import logging
import os
import unittest
from importlib.metadata import entry_points
import time

import yaml

from leaf import start
from leaf.modules.logger_modules.logger_utils import get_logger
from leaf.modules.output_modules.mqtt import MQTT
from paho.mqtt.client import MQTTMessage

from leaf_hello_world import interpreter
from leaf_hello_world.adapter import HelloWorldAdapter

logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)


def run_adapter(instance_data):
    """ Function to create and start the adapter in a separate process """
    output = MQTT("localhost", 1883,username="mcrowther",password="Radeon12300")
    adap = HelloWorldAdapter(output=output, instance_data=instance_data)
    adap.start()


def on_message_test(client, userdata, msg: MQTTMessage):
    """ Callback function when an MQTT message is received """
    logger.info(f"Received message on topic {msg.topic}: {msg.payload}")

    # Ensure userdata is properly passed
    if not hasattr(userdata, "message_received_counter"):
        logger.error("Userdata does not contain message_received_counter")
        return

    # Validate topic
    assert msg.topic == userdata.topic, f"Expected topic {userdata.topic}, but got {msg.topic}"

    # Validate message
    message = json.loads(msg.payload.decode("utf-8"))
    assert message.get('measurement') == 'demo'

    userdata.message_received_counter += 1
    logger.info(f"Message received count: {userdata.message_received_counter}")


class HelloWorldCase(unittest.TestCase):
    def setUp(self) -> None:
        self.topic = None
        self.message_received_counter = 0  # Initialize counter

        logger.info("Clearing log file")
        if os.path.exists("app.log"):
            os.remove("app.log")

        # Load example.yaml
        # curr_dir: str = os.path.dirname(os.path.realpath(__file__))
        # Obtain path from import statement
        curr_dir: str = os.path.dirname(os.path.realpath(interpreter.__file__))
        print(">>>>>>>>> curr_dir: ", curr_dir)

        example_path = os.path.join(curr_dir, "example.yaml")

        try:
            with open(example_path, "r") as file:
                self._config = yaml.safe_load(file)
                logger.info(f"Config loaded: {self._config}")
        except FileNotFoundError:
            self.fail(f"Configuration file {example_path} not found.")

    def tearDown(self) -> None:
        logger.info("Test completed. Cleaning up resources.")

    def test_demo_adapter(self) -> None:
        """ Test HelloWorldAdapter publishing messages to MQTT broker """
        self.output = MQTT("localhost", 1883,username="mcrowther",password="Radeon12300")
        self.instance_data = {
            "instance_id": "test_hello_world",
            "institute": "test_ins",
            "experiment_id": "test_exp",
        }

        self.topic = "test_ins/HelloWorld/test_hello_world/experiment/undefined/measurement/demo"
        self.output.subscribe(self.topic)

        # Pass `self` as `userdata`
        self.output.client.user_data_set(self)
        self.output.client.on_message = on_message_test

        logger.info("Starting HelloWorldAdapter in a separate process...")

        proc = multiprocessing.Process(target=run_adapter, args=(self.instance_data,),daemon=True)
        proc.start()

        # Wait for adapter to run (simulate processing time)
        time.sleep(5)

        # Validate process started
        self.assertIsNotNone(proc, "Adapter process failed to start.")

        # Ensure messages were received
        self.assertGreaterEqual(self.message_received_counter, 5,
                                f"Expected 5 messages, got {self.message_received_counter}")

        # Clean up
        proc.terminate()
        proc.join()
        logger.info("HelloWorldAdapter test completed successfully.")

    def test_demo_interpreter_id(self) -> None:
        """ Test interpreter ID is properly generated """
        inter = interpreter.HelloWorldInterpreter()
        logger.debug(f"Interpreter ID: {inter.id}")

        self.assertIsNotNone(inter.id)
        self.assertIsInstance(inter.id, str)

    def test_load_adapters(self) -> None:
        """ Dynamically load adapters registered via entry points """
        adapters = {}
        for entry_point in entry_points(group="leaf.adapters"):
            adapters[entry_point.name] = entry_point.load()

        logger.warning(f"Adapters loaded: {adapters}")
        self.assertIn("leaf_hello_world", adapters)
        self.assertIsNotNone(adapters["leaf_hello_world"])

    def test_leaf_execution(self):
        """ Test end-to-end execution of the HelloWorldAdapter """
        self.message_received_counter = 0
        self.topic = "WUR/HelloWorld/example_equipment_id1/experiment/undefined/measurement/demo"

        self.output = MQTT("localhost", 1883)
        self.output.subscribe(self.topic)

        # Pass `self` as `userdata`
        self.output.client.user_data_set(self)
        self.output.client.on_message = on_message_test

        logger.info("Starting Leaf execution in a separate process...")
        curr_dir: str = os.path.dirname(os.path.realpath(interpreter.__file__))
        proc = multiprocessing.Process(target=start.main, args=(["--config", curr_dir + "/example.yaml"],),daemon=True)
        proc.start()

        # Wait for process to execute
        proc.join(10)
        # Ensure process is stopped
        proc.terminate()

        # Ensure at least 5 messages were received
        self.assertEqual(self.message_received_counter, 5, f"Expected 5 messages, got {self.message_received_counter}")
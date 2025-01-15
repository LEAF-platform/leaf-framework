# Test for a remote git repository with a hello world example
import logging
import os
import unittest
import threading

from hello_world.adapter import HelloWorldAdapter

from leaf.modules.logger_modules.logger_utils import get_logger
from leaf.modules.output_modules.mqtt import MQTT


logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)

class TestHelloWorld(unittest.TestCase):
    def setUp(self) -> None:
        logger.info("Clearing log file")
        if os.path.exists("app.log"):
            os.remove("app.log")

    def test_hello_world_adapter(self) -> None:
        self.output = MQTT("localhost", 1883)
        # self.output.transmit("test", """'{"test": "test"}""")
        self.instance_data: dict[str, str] = {
            "instance_id": "test_maq",
            "institute": "test_ins",
            "experiment_id": "test_exp",
        }

        logger.info("Starting HelloWorldAdapter...")
        adap = HelloWorldAdapter(output=self.output, instance_data=self.instance_data)
        # Start a new thread for the adapter.start() method
        t = threading.Thread(target=adap.start)
        t.start()
        # Stop the adapter after 5 seconds
        threading.Timer(5, adap.stop).start()
        # Wait for the thread to finish
        t.join()
        # Add assertions
        self.assertIsNotNone(adap)
        logger.info("HelloWorldAdapter started successfully.")
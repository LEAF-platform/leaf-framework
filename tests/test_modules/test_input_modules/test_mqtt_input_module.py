import os
import sys
import unittest
import time
import yaml
from threading import Thread
import csv
from datetime import datetime
import tempfile

sys.path.insert(0, os.path.join(".."))
sys.path.insert(0, os.path.join("..", ".."))
sys.path.insert(0, os.path.join("..", "..", ".."))

from leaf.modules.input_modules.mqtt_watcher import MQTTEventWatcher
from leaf_register.metadata import MetadataManager
from tests.mock_mqtt_client import MockBioreactorClient

curr_dir: str = os.path.dirname(os.path.realpath(__file__))

with open(curr_dir + '/../../test_config.yaml', 'r') as file:
    config = yaml.safe_load(file)

broker = config["OUTPUTS"][0]["broker"]
port = int(config["OUTPUTS"][0]["port"])
try:
    un = config["OUTPUTS"][0]["username"]
    pw = config["OUTPUTS"][0]["password"]
except KeyError:
    un = None
    pw = None

class TestMQTTWatcher(unittest.TestCase):
    def setUp(self):
        self._mock_client = MockBioreactorClient(broker,port,
                                                 username=un,
                                                 password=pw)
        
    def test_mqtt_event_watcher(self):
        metadata_manager = MetadataManager()
        self._adapter = MQTTEventWatcher(metadata_manager=metadata_manager,
                                         broker=broker,
                                         port=port,
                                         username=un,
                                         password=pw,
                                         clientid=None)
    

if __name__ == "__main__":
    unittest.main()

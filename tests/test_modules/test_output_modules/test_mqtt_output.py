import os
import sys
import unittest
import yaml
import time
sys.path.insert(0, os.path.join(".."))
sys.path.insert(0, os.path.join("..",".."))
sys.path.insert(0, os.path.join("..","..",".."))

from core.modules.output_modules.mqtt import MQTT
from mock_mqtt_client import MockBioreactorClient
from core.metadata_manager.metadata import metadata_manager

with open('../../test_config.yaml', 'r') as file:
    config = yaml.safe_load(file)
broker = config["OUTPUT"]["broker_address"]
port = int(config["OUTPUT"]["port"])
un = config["OUTPUT"]["username"]
pw = config["OUTPUT"]["password"]
test_file_dir = "test_dir"
test_file = os.path.join(test_file_dir,"ecoli-GFP-mCherry_inter.csv")


class TestMQTT(unittest.TestCase):
    def setUp(self):
        self._adapter = MQTT(broker,port,username=un,password=pw)
        self._mock_client = MockBioreactorClient(broker,port,
                                                 username=un,password=pw)

    def tearDown(self):
        pass

    def test_transmit(self):
        metadata_manager._metadata["equipment"] = {}
        metadata_manager._metadata["equipment"]["institute"] = "test_transmit"
        metadata_manager._metadata["equipment"]["equipment_id"] = "test_transmit"
        metadata_manager._metadata["equipment"]["instance_id"] = "test_transmit"
        self._mock_client.subscribe("+")
        self._mock_client.subscribe(metadata_manager.experiment.start())
        time.sleep(2)
        self._adapter.transmit(metadata_manager.experiment.start(),
                               {"tst" : "tst"})
        time.sleep(2)
        for k,v in self._mock_client.messages.items():
            if metadata_manager.experiment.start() == k:
                break
        else:
            self.fail()


if __name__ == "__main__":
    unittest.main()
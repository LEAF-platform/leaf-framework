import os
import sys
import unittest
import yaml
import time
sys.path.insert(0, os.path.join(".."))
sys.path.insert(0, os.path.join("..",".."))
sys.path.insert(0, os.path.join("..","..",".."))

from core.modules.output_modules.mqtt import MQTT, logger
from mock_mqtt_client import MockBioreactorClient
from core.metadata_manager.metadata import MetadataManager

# Current location of this script
curr_dir: str = os.path.dirname(os.path.realpath(__file__))



class TestMQTT(unittest.TestCase):
    def setUp(self):
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
        test_file_dir = "test_dir"
        test_file = os.path.join(test_file_dir,"ecoli-GFP-mCherry_inter.csv")

        # Cant get a connetion with the default clientid.
        self._adapter = MQTT(broker,port,username=un,password=pw,
                             clientid=None)
        self._mock_client = MockBioreactorClient(broker,port,
                                                 username=un,
                                                 password=pw)

    def tearDown(self):
        pass

    def test_transmit(self):
        metadata_manager = MetadataManager()
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
            logger.debug(f"Received message: {k} , {v}")
            if metadata_manager.experiment.start() == k:
                break
        else:
            self.fail()


if __name__ == "__main__":
    unittest.main()
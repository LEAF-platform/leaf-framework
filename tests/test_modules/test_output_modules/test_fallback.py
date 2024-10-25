import os
import sys
import unittest
import yaml
import time
import json
import tempfile

sys.path.insert(0, os.path.join(".."))
sys.path.insert(0, os.path.join("..", ".."))
sys.path.insert(0, os.path.join("..", "..", ".."))

from core.modules.output_modules.mqtt import MQTT
from core.modules.output_modules.keydb_client import KEYDB
from core.modules.output_modules.file import FILE
from ...mock_mqtt_client import MockBioreactorClient
from core.metadata_manager.metadata import MetadataManager

# Current location of this script
curr_dir: str = os.path.dirname(os.path.realpath(__file__))

with open(curr_dir + "/../../test_config.yaml", "r") as file:
    config = yaml.safe_load(file)

broker = config["OUTPUTS"][0]["broker"]
port = int(config["OUTPUTS"][0]["port"])
try:
    un = config["OUTPUTS"][0]["username"]
    pw = config["OUTPUTS"][0]["password"]
except KeyError:
    un = None
    pw = None

db_host = "localhost"


class TestMQTT(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_topic = "test_fallback/"
        
        # Create a unique temporary directory for each test to ensure isolation
        self.temp_dir = tempfile.TemporaryDirectory()
        self.file_store_path = os.path.join(self.temp_dir.name, "local.json")
        
        self._file = FILE(self.file_store_path)
        self._keydb: KEYDB = KEYDB(db_host, fallback=self._file)
        self._keydb.connect()
        self._module = MQTT(broker, port, username=un, password=pw, 
                             clientid=None, fallback=self._keydb)
        self._mock_client = MockBioreactorClient(broker, port,
                                                 username=un, 
                                                 password=pw)
        self._mock_client.subscribe(self.mock_topic)
        time.sleep(2)

    def test_fallback_keydb(self) -> None:
        mock_data = {"test_fallback": "test_fallback"}
        self._module.client.loop_stop()
        self._module.client.on_disconnect = None
        self._module.client.loop_start()
        self._module.client.disconnect()
        time.sleep(1)
        self._module.transmit(self.mock_topic, mock_data)
        time.sleep(1)
        self.assertNotIn(self.mock_topic, self._mock_client.messages)
        res = json.loads(self._keydb.retrieve(self.mock_topic))
        self.assertEqual(res, mock_data)

    def test_fallback_file(self) -> None:
        mock_data = {"test_fallback": "test_fallback"}
        self._module.client.loop_stop()
        self._module.client.on_disconnect = None
        self._module.client.loop_start()
        self._module.client.disconnect()
        self._keydb.disconnect()
        time.sleep(1)
        self._module.transmit(self.mock_topic, mock_data)
        time.sleep(1)
        self.assertNotIn(self.mock_topic, self._mock_client.messages)
        res = self._keydb.retrieve(self.mock_topic)
        self.assertEqual(res, None)

        res = [json.loads(n) for n in self._file.retrieve(self.mock_topic)]
        self.assertIn(mock_data, res)


if __name__ == "__main__":
    unittest.main()

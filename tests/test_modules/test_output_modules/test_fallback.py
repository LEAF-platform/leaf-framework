import os
import sys
import unittest
import yaml
import time
import json
import tempfile
from uuid import uuid4

sys.path.insert(0, os.path.join(".."))
sys.path.insert(0, os.path.join("..", ".."))
sys.path.insert(0, os.path.join("..", "..", ".."))

from leaf.modules.output_modules.mqtt import MQTT
from leaf.modules.output_modules.keydb_client import KEYDB
from leaf.modules.output_modules.file import FILE
from tests.mock_mqtt_client import MockBioreactorClient
from leaf_register.metadata import MetadataManager

curr_dir = os.path.dirname(os.path.realpath(__file__))
config_path = os.path.join(curr_dir, "..", "..", "test_config.yaml")

with open(config_path, "r") as file:
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


class TestFallbacks(unittest.TestCase):
    def setUp(self) -> None:
        mock_topic = "test_fallback/"
        
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
        self._mock_client.subscribe(mock_topic)
        time.sleep(2)

    def test_fallback_keydb(self) -> None:
        mock_topic = f"test_fallback/{uuid4()}"
        self._mock_client.subscribe(mock_topic)
        time.sleep(2)

        res = self._keydb.pop(mock_topic)
        mock_data = {"test_fallback": "test_fallback"}
        self._module.client.loop_stop()
        self._module.client.on_disconnect = None
        self._module.client.loop_start()
        self._module.client.disconnect()
        time.sleep(1)
        self._module.transmit(mock_topic, mock_data)
        time.sleep(1)
        self.assertNotIn(mock_topic, self._mock_client.messages)
        res = list(self._keydb.pop(mock_topic))
        self.assertEqual(mock_topic,res[0])
        self.assertEqual(mock_data,json.loads(res[1][0]))

    def test_fallback_file(self) -> None:
        mock_topic = f"test_fallback/{uuid4()}"
        self._mock_client.subscribe(mock_topic)
        time.sleep(2)
        mock_data = {"test_fallback": "test_fallback"}
        self._module.client.loop_stop()
        self._module.client.on_disconnect = None
        self._module.client.loop_start()
        self._module.client.disconnect()
        self._keydb.disconnect()
        time.sleep(1)
        self._module.transmit(mock_topic, mock_data)
        time.sleep(1)
        self.assertNotIn(mock_topic, self._mock_client.messages)
        res = self._keydb.retrieve(mock_topic)
        self.assertEqual(res, None)

        res = [json.loads(n) if not isinstance(n, dict) else n 
               for n in self._file.retrieve(mock_topic) 
               if isinstance(n, dict) or is_valid_json(n)]
        self.assertIn(mock_data, res)

    def test_pop_all_messages(self):
        mock_topic = f"test_fallback/{uuid4()}"
        self._mock_client.subscribe(mock_topic)
        time.sleep(2)
        self._keydb.connect()
        institute = "test_pop_all_messages_institute"
        adapter_id = "test_pop_all_messages_adapter_id"
        instance_id = "test_pop_all_messages_instance_id"
        experiment_id = "test_pop_all_messages_experiment_id"
        measurement_id = "test_pop_all_messages_measurement_id"

        manager = MetadataManager()
        manager.add_equipment_value("adapter_id",adapter_id)
        manager.add_instance_value("institute",institute)
        manager.add_instance_value("instance_id",instance_id)

        inp_messages = {manager.experiment.measurement(experiment_id=experiment_id,
                                                   measurement=measurement_id) : ["A","B","C"],
                        manager.experiment.start() : ["D","E","F"]}
        for topic,messages in inp_messages.items():
            for message in messages:
                self._keydb.transmit(topic,message)
                time.sleep(0.1)
        
        messages = list(self._module.pop_all_messages())
        self.assertTrue(len(messages) > 0)
        for topic,message in messages:
            self.assertIn(topic,inp_messages)
            self.assertIn(message,inp_messages[topic])
        self.assertIsNone(self._keydb.pop())

        for topic,messages in inp_messages.items():
            for message in messages:
                self._file.transmit(topic,message)
                time.sleep(1)
        
        messages = list(self._module.pop_all_messages())
        self.assertTrue(len(messages) > 0)
        for topic,message in messages:
            self.assertIn(topic,inp_messages)
            self.assertIn(message,inp_messages[topic])
        self.assertIsNone(self._file.pop())

    def test_pop_all_messages_multi_output(self):
        mock_topic = f"test_fallback/{uuid4()}"
        self._mock_client.subscribe(mock_topic)
        time.sleep(2)

        self._keydb.connect()
        institute = "test_pop_all_messages_institute"
        adapter_id = "test_pop_all_messages_adapter_id"
        instance_id = "test_pop_all_messages_instance_id"
        experiment_id = "test_pop_all_messages_experiment_id"
        measurement_id = "test_pop_all_messages_measurement_id"

        manager = MetadataManager()
        manager.add_equipment_value("adapter_id",adapter_id)
        manager.add_instance_value("institute",institute)
        manager.add_instance_value("instance_id",instance_id)
        
        keydb_messages = {manager.experiment.measurement(experiment_id=experiment_id,
                                                         measurement=measurement_id) : ["A","B","C"],
                                                         manager.experiment.start() : ["D","E","F"]}
        file_messages = {manager.experiment.measurement(experiment_id=experiment_id,
                                                         measurement=measurement_id) : ["G","H","I"],
                                                         manager.experiment.start() : ["j","K","L"]}
        for topic,messages in keydb_messages.items():
            for message in messages:
                self._keydb.transmit(topic,message)
                time.sleep(0.1)
        for topic,messages in file_messages.items():
            for message in messages:
                self._file.transmit(topic,message)
                time.sleep(0.1)
        
        messages = list(self._module.pop_all_messages())
        for topic,message in messages:
            self.assertTrue(topic in keydb_messages or topic in file_messages)
            self.assertTrue(message in keydb_messages[topic] or message in file_messages[topic])

        self.assertIsNone(self._file.pop())
        self.assertIsNone(self._keydb.pop())
        
def is_valid_json(item):
    try:
        json.loads(item)
        return True
    except json.JSONDecodeError:
        return False
if __name__ == "__main__":
    unittest.main()

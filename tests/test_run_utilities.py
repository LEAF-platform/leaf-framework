import os
import sys
import time
import uuid
import tempfile
import unittest
from threading import Thread
import tempfile
import yaml

sys.path.insert(0, os.path.join(".."))
sys.path.insert(0, os.path.join("..", ".."))
sys.path.insert(0, os.path.join("..", "..", ".."))

from leaf.modules.output_modules.mqtt import MQTT
from leaf.modules.output_modules.keydb_client import KEYDB
from leaf.modules.output_modules.file import FILE
from tests.mock_mqtt_client import MockBioreactorClient
from leaf_register.metadata import MetadataManager
from leaf.utility.running_utilities import handle_disabled_modules
from leaf.start import stop_all_adapters
from leaf.start import run_adapters
from leaf import start
from leaf.error_handler.error_holder import ErrorHolder


curr_dir: str = os.path.dirname(os.path.realpath(__file__))

with open(os.path.join(curr_dir,"test_config.yaml"), "r") as file:
    config = yaml.safe_load(file)

broker = config["OUTPUTS"][0]["broker"]
port = int(config["OUTPUTS"][0]["port"])
try:
    un = config["OUTPUTS"][0]["username"]
    pw = config["OUTPUTS"][0]["password"]
except KeyError:
    un = None
    pw = None

curr_dir = os.path.dirname(os.path.realpath(__file__))
mock_functional_adapter_path = os.path.join(curr_dir,
                                            "mock_functional_adapter")

db_host = "localhost"

class TestRunUtilities(unittest.TestCase):
    def setUp(self) -> None:
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
        time.sleep(2)



    def test_handle_disabled_modules(self):
        self._keydb.connect()
        timeout = 2
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
        
        for topic in inp_messages.keys():
            self._mock_client.subscribe(topic)

        for topic,messages in inp_messages.items():
            for message in messages:
                self._keydb.transmit(topic,message)
                time.sleep(0.1)
        self._module.disable()
        time.sleep(timeout*2)
        handle_disabled_modules(self._module,timeout)
        time.sleep(1)
        for k,v in self._mock_client.messages.items():
            self.assertIn(k,inp_messages)
            self.assertEqual(v,inp_messages[k])
        self.assertIsNone(self._keydb.pop())
        self.assertIsNone(self._file.pop())
        
    '''
    def test_stop_all_adapters(self):
        error_holder = ErrorHolder()
        output = MQTT(
            broker,
            port,
            username=un,
            password=pw,
            clientid=None,
            error_holder=error_holder,
        )
        
        #output = FILE("Tst.tmp")

        write_dir = "test"
        if not os.path.isdir(write_dir):
            os.mkdir(write_dir)
        write_file = os.path.join(write_dir, "tmp1.csv")

        ins = [
            {
                "equipment": {
                    "adapter": "MockFunctionalAdapter",
                    "data": {
                        "instance_id": f"{uuid.uuid4()}",
                        "institute": f"{uuid.uuid4()}",
                    },
                    "requirements": {"write_file": write_file},
                }
            }
        ]

        def _start() -> Thread:
            mthread = Thread(
                target=run_adapters,
                args=[ins, output, error_holder],
                kwargs={"external_adapter": mock_functional_adapter_path},
            )
            mthread.start()
            return mthread

        def _stop(thread: Thread) -> None:
            stop_all_adapters()
            time.sleep(10)

        thread = _start()
        time.sleep(5)
        _stop(thread)
    '''


if __name__ == "__main__":
    unittest.main()

import os
import shutil
import sys
import time
import unittest
from threading import Thread
import tempfile
import yaml
import uuid

sys.path.insert(0, os.path.join(".."))
sys.path.insert(0, os.path.join("..", ".."))
sys.path.insert(0, os.path.join("..", "..", ".."))

from leaf.modules.output_modules.mqtt import MQTT
from leaf.modules.input_modules.file_watcher import FileWatcher
from leaf.modules.phase_modules.measure import MeasurePhase
from leaf.modules.phase_modules.control import ControlPhase
from leaf.modules.process_modules.discrete_module import DiscreteProcess

from leaf.adapters.core_adapters.continuous_experiment_adapter import ContinuousExperimentAdapter
from leaf.adapters.equipment_adapter import AbstractInterpreter

from leaf_register.metadata import MetadataManager
from tests.mock_mqtt_client import MockBioreactorClient
from leaf.error_handler.error_holder import ErrorHolder
from leaf.error_handler.exceptions import AdapterBuildError
from leaf.error_handler.exceptions import HardwareStalledError

curr_dir = os.path.dirname(os.path.realpath(__file__))

with open(os.path.join(curr_dir, "..", "test_config.yaml"), "r") as file:
    config = yaml.safe_load(file)

broker = config["OUTPUTS"][0]["broker"]
port = int(config["OUTPUTS"][0]["port"])
try:
    un = config["OUTPUTS"][0]["username"]
    pw = config["OUTPUTS"][0]["password"]
except KeyError:
    un = None
    pw = None

test_file_dir = os.path.join(curr_dir, "..", "static_files")
initial_file = os.path.join(test_file_dir, "biolector1_metadata.csv")
measurement_file = os.path.join(test_file_dir, "biolector1_measurement.csv")
all_data_file = os.path.join(test_file_dir, "biolector1_full.csv")


def _create_file(watch_file):
    if os.path.isfile(watch_file):
        os.remove(watch_file)
    shutil.copyfile(initial_file, watch_file)
    time.sleep(2)


def _modify_file(watch_file):
    with open(measurement_file, "r") as src:
        content = src.read()
    with open(watch_file, "a") as dest:
        dest.write(content)
    time.sleep(2)


def _delete_file(watch_file):
    if os.path.isfile(watch_file):
        os.remove(watch_file)


class MockBioreactorInterpreter(AbstractInterpreter):
    def __init__(self) -> None:
        super().__init__()
        self.id = "TestBioreactor"

    def metadata(self, data):
        return data

    def measurement(self, data):
        return super().measurement(data)

    def simulate(self):
        return


class MockEquipmentAdapter(ContinuousExperimentAdapter):
    def __init__(self, instance_data,equipment_data, fp,
                 experiment_timeout=None):
        metadata_manager = MetadataManager()
        watcher = FileWatcher(fp, metadata_manager)
        output = MQTT(broker, port, username=un, password=pw, clientid=None)
        error_holder = ErrorHolder()
        metadata_manager.add_instance_data(instance_data)
        super().__init__(
            equipment_data,
            watcher,
            output,
            MockBioreactorInterpreter(),
            metadata_manager=metadata_manager,
            error_holder=error_holder,
            experiment_timeout=experiment_timeout)


class TestEquipmentAdapter(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self._adapter.stop()
        self.temp_dir.cleanup()
        self.mock_client.reset_messages()

    def initialize_experiment(self,**kwargs):
        """
        Helper function to initialize a unique MockEquipmentAdapter
        instance with unique file paths and instance data.
        """

        unique_instance_id = str(uuid.uuid4())
        unique_institute = "TestInstitute_" #+ unique_instance_id[:8]

        unique_file_name = f"TestBioreactor_{unique_instance_id}.txt"
        text_watch_file = os.path.join(self.temp_dir.name, unique_file_name)

        instance_data = {
            "instance_id": unique_instance_id,
            "institute": unique_institute,
        }
        equipment_data = {"adapter_id" : "TestBioreactor_transmit_" + unique_instance_id}
        self.mock_client = MockBioreactorClient(broker, port, username=un, password=pw)

        self._adapter = MockEquipmentAdapter(instance_data,equipment_data,
                                              text_watch_file,**kwargs)

        self.details_topic = self._adapter._metadata_manager.details()
        self.start_topic = self._adapter._metadata_manager.experiment.start()
        self.stop_topic = self._adapter._metadata_manager.experiment.stop()

        self.mock_client.flush(self.details_topic)
        self.mock_client.flush(self.start_topic)
        self.mock_client.flush(self.stop_topic)
        time.sleep(2)
        self.mock_client.subscribe(self.start_topic)
        time.sleep(0.1)
        self.mock_client.subscribe(self.stop_topic)
        time.sleep(0.1)
        self.mock_client.subscribe(self.details_topic)
        time.sleep(2)
    
    
    def tearDown(self):
        try:
            self._adapter.stop()
        except Exception:
            pass

    def test_details(self):
        self.initialize_experiment()
        mthread = Thread(target=self._adapter.start)
        mthread.start()
        time.sleep(2)
        self._adapter.stop()
        mthread.join()
        time.sleep(2)
        self.assertIn(self.details_topic, self.mock_client.messages)
        self.assertEqual(len(self.mock_client.messages[self.details_topic]),1)

    def test_running(self):
        self.initialize_experiment()
        mthread = Thread(target=self._adapter.start)
        mthread.start()
        time.sleep(2)
        # Start Experiment
        self.assertIn(self.start_topic, self.mock_client.messages)
        self._adapter.stop()
        mthread.join()
        time.sleep(2)
        # Stop Experiment
        self.assertIn(self.stop_topic, self.mock_client.messages)






if __name__ == "__main__":
    unittest.main()

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

from leaf.adapters.equipment_adapter import EquipmentAdapter
from leaf.adapters.equipment_adapter import AbstractInterpreter

from leaf_register.metadata import MetadataManager
from tests.mock_mqtt_client import MockBioreactorClient
from leaf.error_handler.error_holder import ErrorHolder

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
        return data

    def simulate(self):
        return


class MockEquipmentAdapter(EquipmentAdapter):
    def __init__(self, instance_data, fp,
                 experiment_timeout=None):
        metadata_manager = MetadataManager()
        watcher = FileWatcher(fp, metadata_manager)
        output = MQTT(broker, port, username=un, password=pw, clientid=None)
        start_p = ControlPhase(
            output, metadata_manager.experiment.start, metadata_manager
        )
        stop_p = ControlPhase(
            output, metadata_manager.experiment.stop, metadata_manager
        )
        measure_p = MeasurePhase(output, metadata_manager)
        details_p = ControlPhase(output, metadata_manager.details, metadata_manager)

        watcher.add_start_callback(start_p.update)
        watcher.add_measurement_callback(measure_p.update)
        watcher.add_stop_callback(stop_p.update)
        watcher.add_initialise_callback(details_p.update)
        phase = [start_p, measure_p, stop_p]
        mock_process = [DiscreteProcess(phase)]
        error_holder = ErrorHolder()
        super().__init__(
            instance_data,
            watcher,
            mock_process,
            MockBioreactorInterpreter(),
            metadata_manager,
            error_holder=error_holder,
            experiment_timeout=experiment_timeout)


# Note the tests haven't been updated here since the rework.
class TestEquipmentAdapter(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self._adapter.stop()
        self.temp_dir.cleanup()
        self.mock_client.reset_messages()

    def initialize_experiment(self):
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

        self.mock_client = MockBioreactorClient(broker, port, username=un, password=pw)

        self._adapter = MockEquipmentAdapter(instance_data, text_watch_file)
        self._adapter._metadata_manager._metadata["equipment"]["equipment_id"] = (
            "TestBioreactor_transmit_" + unique_instance_id
        )

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
        self.assertTrue(len(self.mock_client.messages[self.details_topic]) == 1)

    def test_start(self):
        self.initialize_experiment()
        text_watch_file = os.path.join(self._adapter._watcher._path,
                                       self._adapter._watcher._file_name)
        if self.start_topic in self.mock_client.messages:
            del self.mock_client.messages[self.start_topic]
        mthread = Thread(target=self._adapter.start)
        mthread.start()
        time.sleep(2)
        _create_file(text_watch_file)
        time.sleep(2)
        self._adapter.stop()
        mthread.join()
        time.sleep(2)

        self.assertIn(self.start_topic, self.mock_client.messages)
        self.assertTrue(len(self.mock_client.messages[self.start_topic]) == 1)

    def test_stop(self):
        self.initialize_experiment()
        text_watch_file = os.path.join(self._adapter._watcher._path,
                                       self._adapter._watcher._file_name)
        mthread = Thread(target=self._adapter.start)
        mthread.start()
        time.sleep(2)
        _create_file(text_watch_file)
        self.mock_client.reset_messages()
        time.sleep(2)
        _delete_file(text_watch_file)
        time.sleep(2)
        self._adapter.stop()
        mthread.join()
        time.sleep(2)
        self.assertIn(self.stop_topic, self.mock_client.messages)
        self.assertTrue(len(self.mock_client.messages[self.stop_topic]) == 1)

        self.mock_client.messages = {}
        self.mock_client.unsubscribe(self.start_topic)
        self.mock_client.subscribe(self.start_topic)
        self.assertEqual(self.mock_client.messages, {})

    def test_update(self):
        self.initialize_experiment()
        text_watch_file = os.path.join(self._adapter._watcher._path,
                                       self._adapter._watcher._file_name)
        if os.path.isfile(text_watch_file):
            os.remove(text_watch_file)
        time.sleep(1)
        
        exp_tp = self._adapter._metadata_manager.experiment.measurement(
            experiment_id=self._adapter._interpreter.id, measurement="unknown"
        )
        self.mock_client.subscribe(exp_tp)
        mthread = Thread(target=self._adapter.start)
        mthread.start()
        time.sleep(2)
        _create_file(text_watch_file)
        time.sleep(2)
        _modify_file(text_watch_file)
        time.sleep(2)
        _delete_file(text_watch_file)
        time.sleep(2)
        self._adapter.stop()
        mthread.join()
        time.sleep(2)
        self.assertIn(exp_tp, self.mock_client.messages)


    def test_exceptions(self):
        instance_data = {"instance_id" : "test_exceptions_instance",
                        "institute" : "test_exceptions_ins",
                        "equipment_id" : "test_exceptions_equip"}
        
        test_exp_tw_watch_file = os.path.join("tmp_exception.txt")
        adapter = MockEquipmentAdapter(instance_data,
                                 test_exp_tw_watch_file)
        
        mthread = Thread(target=adapter.start)
        mthread.start()
        with open(measurement_file, 'r') as src:
            content = src.read()
        with open(test_exp_tw_watch_file, 'a') as dest:
            dest.write(content)
        time.sleep(2)
        adapter.stop()
        mthread.join()
    
    def test_experiment_timeout(self):
        instance_data = {"instance_id" : "test_experiment_timeout_instance",
                        "institute" : "test_experiment_timeout_ins",
                        "equipment_id" : "test_experiment_timeout_equip"}
        temp_dir = tempfile.TemporaryDirectory()
        test_exp_tw_watch_file = os.path.join(temp_dir.name,"tmp_test_experiment_timeout.txt")

        exp_timeout = 1
        adapter = MockEquipmentAdapter(instance_data,
                                 test_exp_tw_watch_file,
                                 experiment_timeout=exp_timeout)
        
        mthread = Thread(target=adapter.start)
        mthread.start()
        _create_file(test_exp_tw_watch_file)
        time.sleep(3)
        adapter.stop()
        mthread.join()

if __name__ == "__main__":
    unittest.main()

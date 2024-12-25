import os
import sys
import unittest
from threading import Thread
import yaml
import time
import shutil
import tempfile

sys.path.insert(0, os.path.join(".."))
sys.path.insert(0, os.path.join("..",".."))
sys.path.insert(0, os.path.join("..","..",".."))

from leaf.modules.output_modules.mqtt import MQTT
from leaf.modules.input_modules.file_watcher import FileWatcher
from leaf.modules.phase_modules.measure import MeasurePhase
from leaf.modules.phase_modules.start import StartPhase
from leaf.modules.phase_modules.stop import StopPhase
from leaf.modules.process_modules.continous_module import ContinousProcess
from leaf.modules.process_modules.discrete_module import DiscreteProcess
from tests.mock_mqtt_client import MockBioreactorClient
from leaf_register.metadata import MetadataManager

# Current location of this script
curr_dir: str = os.path.dirname(os.path.realpath(__file__))

with open(curr_dir + '/../../test_config.yaml', 'r') as file:
    config = yaml.safe_load(file)

broker = config["OUTPUTS"][0]["broker"]
port = int(config["OUTPUTS"][0]["port"])

try:
    un = config["OUTPUTS"][0]["username"]
    pw = config["OUTPUTS"][0]["password"]
except:
    un = None
    pw = None

test_file_dir = os.path.join(curr_dir, "..", "..", "static_files")
initial_file = os.path.join(test_file_dir, "biolector1_metadata.csv")
measurement_file = os.path.join(test_file_dir, "biolector1_measurement.csv")


def _create_file(text_watch_file):
    shutil.copyfile(initial_file, text_watch_file)
    time.sleep(1)


def _modify_file(text_watch_file):
    with open(measurement_file, 'r') as src:
        content = src.read()
    with open(text_watch_file, 'a') as dest:
        dest.write(content)
    time.sleep(1)


def _delete_file(text_watch_file):
    if os.path.isfile(text_watch_file):
        os.remove(text_watch_file)


def _run_change(func, text_watch_file) -> None:
    mthread = Thread(target=func, args=(text_watch_file,))
    mthread.start()
    mthread.join()

class MockInterpreter:
    def __init__(self,experiment_id):
        self.id = experiment_id

    def metadata(self,data):
        return [data]
    def measurement(self,data):
        return [data]
    
class TestContinousProcess(unittest.TestCase):
    def setUp(self) -> None:
        # Use a temporary file for each test to avoid interference
        self.text_watch_file = tempfile.NamedTemporaryFile(delete=False).name

        self.mock_client = MockBioreactorClient(broker, port, username=un, password=pw)
        self.mock_client.subscribe(f'test_transmit/test_transmit/test_transmit/#')

        self.metadata_manager = MetadataManager()
        self.metadata_manager._metadata["equipment"] = {}
        self.metadata_manager._metadata["equipment"]["institute"] = "test_transmit"
        self.metadata_manager._metadata["equipment"]["equipment_id"] = "test_transmit"
        self.metadata_manager._metadata["equipment"]["instance_id"] = "test_transmit"

        self.watcher = FileWatcher(self.text_watch_file, self.metadata_manager)
        output = MQTT(broker, port, username=un, password=pw, clientid=None)
        self._phase = MeasurePhase(self.metadata_manager)
        self._module = ContinousProcess(output,self._phase)

        self._mock_experiment = "test_experiment_id"
        self._mock_measurement = "test_measurement_id"
        self._module.set_interpreter(MockInterpreter(self._mock_experiment))
        self.watcher.add_callback(self._mock_update)

    def tearDown(self) -> None:
        self.watcher.stop()
        time.sleep(1)
        if os.path.isfile(self.text_watch_file):
            os.remove(self.text_watch_file)
        self.mock_client = None

    def _mock_update(self, topic, data) -> None:
        self._module.process_input(topic,data)

    def test_continous_process(self) -> None:
        _run_change(_create_file, self.text_watch_file)
        self.watcher.start()
        time.sleep(1)
        _run_change(_modify_file, self.text_watch_file)
        time.sleep(1)
        for k, v in self.mock_client.messages.items():
            if self.metadata_manager.experiment.measurement(experiment_id=self._mock_experiment,
                                                            measurement="unknown") == k:
                break
        else:
            self.fail()


class TestDiscreteProcess(unittest.TestCase):
    def setUp(self) -> None:
        # Use a temporary file for each test to avoid interference
        self.text_watch_file = tempfile.NamedTemporaryFile(delete=False).name

        self.mock_client = MockBioreactorClient(broker, port, username=un, password=pw)
        self.mock_client.subscribe(f'test_transmit/test_transmit/test_transmit/#')

        self.metadata_manager = MetadataManager()
        self.metadata_manager._metadata["equipment"] = {}
        self.metadata_manager._metadata["equipment"]["institute"] = "test_transmit"
        self.metadata_manager._metadata["equipment"]["equipment_id"] = "test_transmit"
        self.metadata_manager._metadata["equipment"]["instance_id"] = "test_transmit"

        self.watcher = FileWatcher(self.text_watch_file, 
                                   self.metadata_manager)
        output = MQTT(broker, port, username=un, password=pw, 
                      clientid=None)

        start_p = StartPhase(self.metadata_manager)
        stop_p = StopPhase(self.metadata_manager)
        self._measure_p = MeasurePhase(self.metadata_manager)

        phases = [start_p, self._measure_p, stop_p]
        self._module = DiscreteProcess(output,phases)
        self._mock_experiment = "test_experiment_id"
        self._mock_measurement = "test_measurement_id"
        self._module.set_interpreter(MockInterpreter(self._mock_experiment))
        self.watcher.add_callback(self._module.process_input)

    def tearDown(self) -> None:
        self.watcher.stop()
        time.sleep(1)
        if os.path.isfile(self.text_watch_file):
            os.remove(self.text_watch_file)
        self.mock_client = None

    def test_discrete_process(self):
        if os.path.isfile(self.text_watch_file):
            os.remove(self.text_watch_file)
            time.sleep(0.5)
        self.watcher.start()
        time.sleep(1)
        _run_change(_create_file, self.text_watch_file)
        time.sleep(1)
        _run_change(_modify_file, self.text_watch_file)
        time.sleep(1)
        _run_change(_delete_file, self.text_watch_file)
        time.sleep(1)
        
        for k, v in self.mock_client.messages.items():
            if self.metadata_manager.experiment.start() == k:
                break
        else:
            self.fail()

        for k, v in self.mock_client.messages.items():
            if self.metadata_manager.experiment.measurement(experiment_id=self._mock_experiment,
                                                            measurement="unknown") == k:
                break
        else:
            self.fail()

        for k, v in self.mock_client.messages.items():
            if self.metadata_manager.experiment.stop() == k:
                break
        else:
            self.fail()

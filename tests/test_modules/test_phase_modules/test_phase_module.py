import os
import sys
import unittest
from threading import Thread
import yaml
import time
import shutil
import tempfile

sys.path.insert(0, os.path.join(".."))
sys.path.insert(0, os.path.join("..", ".."))
sys.path.insert(0, os.path.join("..", "..", ".."))

from leaf.modules.output_modules.mqtt import MQTT
from leaf.modules import FileWatcher
from leaf.modules.phase_modules.measure import MeasurePhase
from leaf.modules import ControlPhase
from ...mock_mqtt_client import MockBioreactorClient
from leaf.metadata_manager.metadata import MetadataManager

# Current location of this script
curr_dir: str = os.path.dirname(os.path.realpath(__file__))

with open(curr_dir + "/../../test_config.yaml", "r") as file:
    config = yaml.safe_load(file)

broker = config["OUTPUTS"][0]["broker"]
port = int(config["OUTPUTS"][0]["port"])

try:
    un = config["OUTPUTS"][0]["username"]
    pw = config["OUTPUTS"][0]["password"]
except:
    un = None
    pw = None

curr_dir = os.path.dirname(os.path.realpath(__file__))
test_file_dir = os.path.join(curr_dir, "..", "..", "static_files")
initial_file = os.path.join(test_file_dir, "biolector1_metadata.csv")
measurement_file = os.path.join(test_file_dir, "biolector1_measurement.csv")


def _create_file(text_watch_file) -> None:
    shutil.copyfile(initial_file, text_watch_file)
    time.sleep(2)


def _modify_file(text_watch_file) -> None:
    with open(measurement_file, "r") as src:
        content = src.read()
    with open(text_watch_file, "a") as dest:
        dest.write(content)
    time.sleep(2)


def _run_change(func, text_watch_file) -> None:
    mthread = Thread(target=func, args=(text_watch_file,))
    mthread.start()
    mthread.join()


class TestMeasurePhase(unittest.TestCase):
    def setUp(self) -> None:
        # Create a unique temporary file for this test
        self.text_watch_file = tempfile.NamedTemporaryFile(delete=False).name

        self._metadata_manager = MetadataManager()
        self._metadata_manager._metadata["equipment"] = {}
        self._metadata_manager._metadata["equipment"]["institute"] = "test_transmit"
        self._metadata_manager._metadata["equipment"]["equipment_id"] = "test_transmit"
        self._metadata_manager._metadata["equipment"]["instance_id"] = "test_transmit"

        self.watcher = FileWatcher(self.text_watch_file, self._metadata_manager)
        output = MQTT(broker, port, username=un, password=pw, clientid=None)
        self._module = MeasurePhase(output, self._metadata_manager)

        # Ensure unique experiment_id and measurement_id for each test instance
        self._mock_experiment = f"test_experiment_id_{self._testMethodName}"
        self._mock_measurement = f"test_measurement_id_{self._testMethodName}"

        self.watcher.add_measurement_callback(self._mock_update)

        # Initialize the mock MQTT client and subscribe to a hardcoded topic
        self.mock_client = MockBioreactorClient(broker, port, username=un, password=pw)
        self.mock_client.subscribe(f"test_transmit/test_transmit/test_transmit/#")

    def tearDown(self) -> None:
        # Clean up the temporary file
        if os.path.isfile(self.text_watch_file):
            os.remove(self.text_watch_file)
        self.mock_client = None

    def _mock_update(self, data: str) -> None:
        self._module.update(
            experiment_id=self._mock_experiment,
            measurement=self._mock_measurement,
            data=data,
        )

    def test_measure_phase(self) -> None:
        _create_file(self.text_watch_file)
        proxy_thread = Thread(target=self.watcher.start)
        proxy_thread.start()
        time.sleep(2)
        _run_change(_modify_file, self.text_watch_file)
        time.sleep(2)
        proxy_thread.join()

        # Check if the messages received match the expected measurement
        for k, v in self.mock_client.messages.items():
            if (
                self._metadata_manager.experiment.measurement(
                    experiment_id=self._mock_experiment,
                    measurement=self._mock_measurement,
                )
                == k
            ):
                break
        else:
            self.fail()


class TestControlPhase(unittest.TestCase):
    def setUp(self) -> None:
        # Create a unique temporary file for this test
        self.text_watch_file = tempfile.NamedTemporaryFile(delete=False).name

        self._metadata_manager = MetadataManager()
        self._metadata_manager._metadata["equipment"] = {}
        self._metadata_manager._metadata["equipment"]["institute"] = "test_transmit"
        self._metadata_manager._metadata["equipment"]["equipment_id"] = "test_transmit"
        self._metadata_manager._metadata["equipment"]["instance_id"] = "test_transmit"

        self.watcher = FileWatcher(self.text_watch_file, self._metadata_manager)
        output = MQTT(broker, port, username=un, password=pw, clientid=None)
        self._module = ControlPhase(
            output, self._metadata_manager.experiment.start, self._metadata_manager
        )
        self.watcher.add_start_callback(self._module.update)

        # Initialize a unique mock MQTT client
        self.mock_client = MockBioreactorClient(broker, port, username=un, password=pw)
        self.mock_client.subscribe(
            f'test_transmit/test_transmit/{self._metadata_manager._metadata["equipment"]["equipment_id"]}/#'
        )

    def tearDown(self) -> None:
        # Clean up the temporary file
        self.watcher.stop()
        if os.path.isfile(self.text_watch_file):
            os.remove(self.text_watch_file)
        self.mock_client = None

    def test_control_phase(self) -> None:
        proxy_thread = Thread(target=self.watcher.start)
        proxy_thread.start()
        time.sleep(2)
        _run_change(_create_file, self.text_watch_file)
        time.sleep(2)
        proxy_thread.join()
        for k, v in self.mock_client.messages.items():
            if self._metadata_manager.experiment.start() == k:
                break
        else:
            self.fail()

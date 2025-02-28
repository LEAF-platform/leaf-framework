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

from leaf.modules.phase_modules.measure import MeasurePhase
from leaf.modules.phase_modules.control import ControlPhase
from leaf.modules.phase_modules.start import StartPhase
from leaf.modules.phase_modules.initialisation import InitialisationPhase
from leaf.modules.phase_modules.stop import StopPhase
from leaf_register.metadata import MetadataManager

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

class TestMeasurePhase(unittest.TestCase):
    def setUp(self) -> None:
        # Create a unique temporary file for this test
        self.text_watch_file = tempfile.NamedTemporaryFile(delete=False).name

        self._metadata_manager = MetadataManager()
        self._metadata_manager.add_equipment_value("adapter_id","test_transmit")
        self._metadata_manager.add_instance_value("institute","test_transmit")
        self._metadata_manager.add_instance_value("instance_id","test_transmit")

        self._module = MeasurePhase(metadata_manager=self._metadata_manager)

        # Ensure unique experiment_id and measurement_id for each test instance
        self._mock_experiment = f"test_experiment_id_{self._testMethodName}"
        self._mock_measurement = f"test_measurement_id_{self._testMethodName}"

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
        with open(measurement_file, "r") as src:
            content = src.read()
        
        results = self._module.update(content,
                                      experiment_id=self._mock_experiment,
                                      measurement=self._mock_measurement)
        for k,v in results:
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

    def test_is_activated(self):
        topic = self._metadata_manager.experiment.measurement
        self.assertTrue(self._module.is_activated(topic))
        topic = self._metadata_manager.experiment.start
        self.assertFalse(self._module.is_activated(topic))

    def test_measure_phase_max_measurement(self):
        self._module._maximum_message_size = 10
        exp_id = "test_measure_phase_max_measurement"
        class MockInterpreter:
            def __init__(self):
                self.id = exp_id
            def measurement(self,data):
                return list(range(1, 24 + 1))
            
        interpreter = MockInterpreter()
        self._module.set_interpreter(interpreter)
        
        with open(measurement_file, "r") as src:
            content = src.read()
        measurement_messages = self._module.update(content)
        exp_id = self._metadata_manager.experiment.measurement(experiment_id=exp_id,
                                                               measurement="unknown")
        expected_chunks = [
            list(range(1, 11)),
            list(range(11, 21)),
            list(range(21, 25))
        ]
        self.assertEqual(len(measurement_messages), len(expected_chunks))
        for chunk, expected_chunk in zip(measurement_messages, expected_chunks):
            self.assertEqual(chunk[1], expected_chunk)

class TestControlPhase(unittest.TestCase):
    def setUp(self) -> None:
        self._metadata_manager = MetadataManager()
        self._metadata_manager.add_equipment_value("adapter_id","test_transmit")
        self._metadata_manager.add_instance_value("institute","test_transmit")
        self._metadata_manager.add_instance_value("instance_id","test_transmit")
        self._module = ControlPhase(self._metadata_manager.experiment.start, 
                                    metadata_manager=self._metadata_manager)

    def tearDown(self) -> None:
        pass

    def test_control_phase(self) -> None:
        res = self._module.update()
        self.assertEqual(res,[(self._metadata_manager.experiment.start(),None)])
    



class TestStartPhase(unittest.TestCase):
    def setUp(self) -> None:
        self._metadata_manager = MetadataManager()
        self._metadata_manager.add_equipment_value("adapter_id","test_transmit")
        self._metadata_manager.add_instance_value("institute","test_transmit")
        self._metadata_manager.add_instance_value("instance_id","test_transmit")
        self._module = StartPhase(metadata_manager=self._metadata_manager)
        
    def test_start_phase(self) -> None:
        data = {}
        res = self._module.update(data)
        
        expected_values = {self._metadata_manager.experiment.start() : data,
                           self._metadata_manager.experiment.stop() : None,
                           self._metadata_manager.running() : True}

        for k,v in expected_values.items():
            self.assertIn((k,v),res)

class TestStopPhase(unittest.TestCase):
    def setUp(self) -> None:
        self._metadata_manager = MetadataManager()
        self._metadata_manager.add_equipment_value("adapter_id","test_transmit")
        self._metadata_manager.add_instance_value("institute","test_transmit")
        self._metadata_manager.add_instance_value("instance_id","test_transmit")
        self._module = StopPhase(metadata_manager=self._metadata_manager)
        
    def test_stop_phase(self) -> None:
        data = {}
        res = self._module.update(data)
        
        expected_values = {self._metadata_manager.experiment.start() : None,
                           self._metadata_manager.experiment.stop() : data,
                           self._metadata_manager.running() : False}

        for k,v in expected_values.items():
            self.assertIn((k,v),res)

class TestInitialisationPhase(unittest.TestCase):
    def setUp(self) -> None:
        self._metadata_manager = MetadataManager()
        self._metadata_manager.add_equipment_value("adapter_id","test_transmit")
        self._metadata_manager.add_instance_value("institute","test_transmit")
        self._metadata_manager.add_instance_value("instance_id","test_transmit")
        self._module = InitialisationPhase(metadata_manager=self._metadata_manager)
        
    def test_initialisation_phase(self) -> None:
        data = {}
        res = self._module.update(data)
        
        expected_values = {self._metadata_manager.details() : data}

        for k,v in expected_values.items():
            self.assertIn((k,v),res)
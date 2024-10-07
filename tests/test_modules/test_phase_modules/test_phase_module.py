import os
import sys
import unittest
from threading import Thread
import yaml
import time
import shutil
sys.path.insert(0, os.path.join(".."))
sys.path.insert(0, os.path.join("..",".."))
sys.path.insert(0, os.path.join("..","..",".."))

from core.modules.output_modules.mqtt import MQTT
from core.modules.input_modules.file_watcher import FileWatcher
from core.modules.measurement_modules.biomass import Biomass
from core.modules.measurement_modules.o2 import O2
from core.modules.measurement_modules.ph import pH
from core.modules.phase_modules.measure import MeasurePhase
from core.modules.phase_modules.control import ControlPhase

from mock_mqtt_client import MockBioreactorClient
from core.metadata_manager.metadata import metadata_manager

with open('../../test_config.yaml', 'r') as file:
    config = yaml.safe_load(file)
broker = config["OUTPUT"]["broker_address"]
port = int(config["OUTPUT"]["port"])
un = config["OUTPUT"]["username"]
pw = config["OUTPUT"]["password"]

curr_dir = os.path.dirname(os.path.realpath(__file__))
test_file_dir = os.path.join(curr_dir,"..","static_files")
test_file = os.path.join(test_file_dir,"ecoli-GFP-mCherry_inter.csv")
initial_file = os.path.join(test_file_dir,"biolector1_metadata.csv")
measurement_file = os.path.join(test_file_dir,"biolector1_measurement.csv")
text_watch_file = os.path.join("tmp.csv")

mock_client = MockBioreactorClient(broker,port,username=un,password=pw)
mock_client.subscribe("#")

def _create_file():
    shutil.copyfile(initial_file, text_watch_file)
    time.sleep(2)

def _modify_file():
    with open(measurement_file, 'r') as src:
        content = src.read()
    with open(text_watch_file, 'a') as dest:
        dest.write(content)
    time.sleep(2)

def _run_change(func):
    mthread = Thread(target=func)
    mthread.start()
    mthread.join()


class TestMeasurePhase(unittest.TestCase):
    def setUp(self):
        metadata_manager._metadata["equipment"] = {}
        metadata_manager._metadata["equipment"]["institute"] = "test_transmit"
        metadata_manager._metadata["equipment"]["equipment_id"] = "test_transmit"
        metadata_manager._metadata["equipment"]["instance_id"] = "test_transmit"
        self.watcher = FileWatcher(text_watch_file)
        output = MQTT(broker,port,username=un,password=pw)
        measurements = [Biomass(),O2(),pH()]
        self._module = MeasurePhase(output,
                                    metadata_manager.experiment.measurement,
                                    measurements)
        self._mock_experiment="test_experiment_id"
        self.watcher.add_measurement_callback(self._mock_update)

    def tearDown(self):
        self.watcher.stop()
        if os.path.isfile(text_watch_file):
            os.remove(text_watch_file)

    def _mock_update(self,data):
        self._module.update(experiment_id=self._mock_experiment,
                             data=data)

    def test_measure_phase(self):
        _create_file()
        proxy_thread = Thread(target=self.watcher.start)
        proxy_thread.start()
        time.sleep(2)
        _run_change(_modify_file)
        time.sleep(2)
        proxy_thread.join()
        for k,v in mock_client.messages.items():
            if metadata_manager.experiment.measurement(experiment_id=self._mock_experiment) == k:
                break
        else:
            self.fail()


class TestControlPhase(unittest.TestCase):
    def setUp(self):
        metadata_manager._metadata["equipment"] = {}
        metadata_manager._metadata["equipment"]["institute"] = "test_transmit"
        metadata_manager._metadata["equipment"]["equipment_id"] = "test_transmit"
        metadata_manager._metadata["equipment"]["instance_id"] = "test_transmit"
        self.watcher = FileWatcher(text_watch_file)
        output = MQTT(broker,port,username=un,password=pw)
        self._module = ControlPhase(output,
                                    metadata_manager.experiment.start)
        self.watcher.add_start_callback(self._module.update)

    def tearDown(self):
        self.watcher.stop()
        if os.path.isfile(text_watch_file):
            os.remove(text_watch_file)

    def test_control_phase(self):
        proxy_thread = Thread(target=self.watcher.start)
        proxy_thread.start()
        time.sleep(2)
        _run_change(_create_file)
        time.sleep(2)
        proxy_thread.join()
        for k,v in mock_client.messages.items():
            if metadata_manager.experiment.start() == k:
                break
        else:
            self.fail()



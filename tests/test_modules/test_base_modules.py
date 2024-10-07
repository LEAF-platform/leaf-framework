import os
import sys
import unittest

sys.path.insert(0, os.path.join(".."))
sys.path.insert(0, os.path.join("..",".."))

from core.modules.phase_modules.phase import PhaseModule
from core.modules.input_modules.event_watcher import EventWatcher
from core.modules.measurement_modules.measurement_module import MeasurementModule
from core.modules.output_modules.output_module import OutputModule
from core.modules.process_modules.process_module import ProcessModule

class MockEventWatcherModule(EventWatcher):
    def __init__(self):
        super().__init__()

    def start(self):
        return True

class TestEventWatcherModule(unittest.TestCase):
    def setUp(self):
        self._adapter = MockEventWatcherModule()
    
    def test_functions(self):
        def test_func():
            return False
        self.assertTrue(self._adapter.start())
        self._adapter.measurement_callback = test_func
        self.assertFalse(self._adapter.measurement_callback())
        
           
class MockOutputModule(OutputModule):
    def __init__(self):
        super().__init__()

    def experiment_measurement(self, data):
        return data

class TestOutputModule(unittest.TestCase):
    def setUp(self):
        self._adapter = MockOutputModule()

    def test_functions(self):
        self.assertEqual(self._adapter.experiment_measurement(True),
                         True)


class MockMeasurementModule(MeasurementModule):
    def __init__(self):
        super().__init__()

    def transform(self,data):
        return data

class TestMeasurementModule(unittest.TestCase):
    def setUp(self):
        self._adapter = MockMeasurementModule()
    
    def test_functions(self):
        self.assertEqual(self._adapter.transform(True),
                         True)


class MockPhaseModule(PhaseModule):
    def __init__(self):
        super().__init__(MockEventWatcherModule(),
                         MockOutputModule(),
                         MockMeasurementModule())

    def update(self,data):
        return data

class TestPhaseModule(unittest.TestCase):
    def setUp(self):
        self._adapter = MockPhaseModule()
    
    def test_functions(self):
        self.assertEqual(self._adapter.update(True),
                         True)
        
        
class MockProcessModule(ProcessModule):
    def __init__(self):
        mock_phase = MockPhaseModule()
        super().__init__(mock_phase,)

class TestProcessModule(unittest.TestCase):
    def setUp(self):
        self._adapter = MockProcessModule()
    
    def test_functions(self):
        self._adapter.set_interpreter(None)
        self.assertIsNone(self._adapter._phases[0]._interpreter)

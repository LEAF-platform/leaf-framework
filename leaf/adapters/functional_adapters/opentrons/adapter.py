import os
from threading import Thread
import time
from typing import Optional

from leaf.adapters.core_adapters.start_stop_adapter import StartStopAdapter
from leaf.adapters.functional_adapters.opentrons.interpreter import OpentronsInterpreter
from leaf.modules.process_modules.discrete_module import DiscreteProcess
from leaf.modules.phase_modules.start import StartPhase
from leaf.modules.phase_modules.stop import StopPhase
from leaf.modules.phase_modules.measure import MeasurePhase
from leaf.modules.phase_modules.initialisation import InitialisationPhase
from leaf.modules.input_modules.csv_watcher import CSVWatcher
from leaf.metadata_manager.metadata import MetadataManager
from leaf.error_handler.exceptions import AdapterLogicError, SeverityLevel
from leaf.error_handler.error_holder import ErrorHolder
from leaf.modules.output_modules.output_module import OutputModule

current_dir = os.path.dirname(os.path.abspath(__file__))
metadata_fn = os.path.join(current_dir, "adapter.json")

class OpentronsAdapter(StartStopAdapter):
    def __init__(self,instance_data: dict,output: OutputModule,
                 stagger_transmit: bool = False, 
                 error_holder: Optional[ErrorHolder] = None):
        
        metadata_manager = MetadataManager()
        # Its unsure what this mechanism will be.
        watcher = CSVWatcher(write_file, metadata_manager)

        interpreter = OpentronsInterpreter(error_holder=error_holder)
        super().__init__(instance_data,watcher,output,interpreter,
                         stagger_transmit=stagger_transmit,error_holder=error_holder,
                         metadata_manager=metadata_manager)
        
        self._metadata_manager.add_equipment_data(metadata_fn)

    def simulate(self, filepath: str, wait: Optional[int] = None, 
                 delay: Optional[int] = None) -> None:
        """
        Simulate/Mock an experiment within the Biolector using existing data.

        Args:
            filepath: Path to the CSV file that provides input data.
            wait: Time (in seconds) to wait between measurements
            delay: Optional delay (in seconds) before starting the simulation.

        Raises:
            ValueError: If the write file already exists, to prevent overwriting.
        """
        pass

import os
from threading import Thread
import time
from typing import Optional

from leaf.adapters.core_adapters.start_stop_adapter import StartStopAdapter
from leaf.adapters.functional_adapters.biolector1.interpreter import (
    Biolector1Interpreter,
)
from leaf.modules.input_modules.csv_watcher import CSVWatcher
from leaf.metadata_manager.metadata import MetadataManager
from leaf.error_handler.exceptions import AdapterLogicError, SeverityLevel
from leaf.error_handler.error_holder import ErrorHolder
from leaf.modules.output_modules.output_module import OutputModule

current_dir = os.path.dirname(os.path.abspath(__file__))
metadata_fn = os.path.join(current_dir, "device.json")
        
class Biolector1Adapter(StartStopAdapter):
    """
    Adapter class for Biolector1, a discrete bioreactor with microwell plates.
    """

    def __init__(self,instance_data: dict,output: OutputModule,
        write_file: Optional[str] = None,stagger_transmit: bool = False,
        error_holder: Optional[ErrorHolder] = None):
        """
        Initialise Biolector1Adapter, setting up phases, process adapters, and metadata.

        Args:
            instance_data: Data specific to this bioreactor instance.
            output: The OutputModule responsible for handling and transmitting data.
            write_file: The file that the CSVWatcher will watch and the biolector machine writes to.
            stagger_transmit: If True, transmits data in staggered intervals. Set True for large measurements.
        """
        metadata_manager = MetadataManager()
        watcher = CSVWatcher(write_file, metadata_manager)

        interpreter = Biolector1Interpreter(error_holder=error_holder)
        super().__init__(instance_data,watcher,output,interpreter,
                         stagger_transmit=stagger_transmit,error_holder=error_holder,
                         metadata_manager=metadata_manager)
        self._write_file: Optional[str] = write_file
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
        if wait is None:
            wait = 10

        if os.path.isfile(self._write_file):
            exception = AdapterLogicError(
                "Trying to run test when the file exists.",
                severity=SeverityLevel.CRITICAL,
            )
            if self._error_holder is not None:
                self._error_holder.add_error(exception)
            else:
                raise exception

        proxy_thread = Thread(target=self.start)
        proxy_thread.start()

        if delay is not None:
            print(f"Delay for {delay} seconds.")
            time.sleep(delay)
            print("Delay finished.")

        self._interpreter.simulate(filepath, self._write_file, wait)
        time.sleep(wait)

        os.remove(self._write_file)

        self.stop()
        proxy_thread.join()

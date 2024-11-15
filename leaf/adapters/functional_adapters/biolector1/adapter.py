import os
from threading import Thread
import time
from typing import Optional

from leaf.adapters.core_adapters.bioreactor import Bioreactor
from leaf.adapters.functional_adapters.biolector1.interpreter import (
    Biolector1Interpreter,
)
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
metadata_fn = os.path.join(current_dir, "device.json")

class Biolector1Adapter(Bioreactor):
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

        start_p = StartPhase(output, metadata_manager)
        stop_p = StopPhase(output, metadata_manager)
        measure_p = MeasurePhase(output, metadata_manager, 
                                 stagger_transmit=stagger_transmit)
        details_p = InitialisationPhase(output, metadata_manager)

        # Trigger start phase when experiment starts
        watcher.add_start_callback(start_p.update)  
        # Trigger measure phase when measurement is taken.
        watcher.add_measurement_callback(measure_p.update)
        # Trigger stop phase when experiment stops.
        watcher.add_stop_callback(stop_p.update)
        # Trigger initialization phase when adapter starts.
        watcher.add_initialise_callback(details_p.update)

        phase = [start_p, measure_p, stop_p]
        process = [DiscreteProcess(phase)]

        interpreter = Biolector1Interpreter(error_holder=error_holder)
        super().__init__(
            instance_data,
            watcher,
            process,
            interpreter,
            metadata_manager=metadata_manager,
            error_holder=error_holder,
        )

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

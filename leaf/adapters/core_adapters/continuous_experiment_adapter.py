from typing import Optional
import time

from leaf.modules.process_modules.continous_module import ContinousProcess
from leaf.modules.process_modules.process_module import ProcessModule
from leaf.modules.phase_modules.start import StartPhase
from leaf.modules.phase_modules.stop import StopPhase
from leaf.modules.phase_modules.measure import MeasurePhase
from leaf.modules.phase_modules.initialisation import InitialisationPhase
from leaf_register.metadata import MetadataManager
from leaf.error_handler.error_holder import ErrorHolder
from leaf.modules.input_modules.event_watcher import EventWatcher
from leaf.modules.output_modules.output_module import OutputModule
from leaf.adapters.equipment_adapter import EquipmentAdapter
from leaf.adapters.equipment_adapter import AbstractInterpreter


class ContinuousExperimentAdapter(EquipmentAdapter):
    """
    Adapter that implements a continous process workflow i.e. for equipment 
    that doesn't have defined experiments.

    It initializes and manages discrete phases for starting, stopping,
    measuring, and initializing equipment processes. The start phase is initialised 
    when the adapter starts and end phase before the adapter exits.
    """

    def __init__(
        self,
        instance_data: str,
        watcher: EventWatcher,
        output: OutputModule,
        interpreter: AbstractInterpreter,
        maximum_message_size: Optional[int] = 1,
        error_holder: Optional[ErrorHolder] = None,
        metadata_manager: Optional[MetadataManager] = None,
        experiment_timeout: Optional[int] = None,
    ):
        """
        Initialize the ContinuousAdapter with its phases and processes.

        Args:
            instance_data (str): Data related to the instance.
            watcher (EventWatcher): The input module used to watch or monitor events or data.
            output (OutputModule): The output module used to transmit data.
            interpreter (AbstractInterpreter): The interpreter for processing data.
            maximum_message_size (Optional[int]): The maximum size of messages in the MeasurePhase.
            error_holder (Optional[ErrorHolder]): Object to store and manage errors.
            metadata_manager (Optional[MetadataManager]): The metadata manager for equipment data.
            experiment_timeout (Optional[int]): Timeout for experiments in seconds.
        """
        # Initialize phases
        start_p = StartPhase(metadata_manager)
        stop_p = StopPhase(metadata_manager)
        measure_p = MeasurePhase(
            metadata_manager, maximum_message_size=maximum_message_size
        )
        details_p = InitialisationPhase(metadata_manager)

        # Combine phases into a discrete process
        measurement_process = ContinousProcess(output, measure_p,
                                            metadata_manager=metadata_manager,
                                            error_holder=error_holder)
        self._control_process = ProcessModule(output,[start_p, stop_p, details_p],
                                            metadata_manager=metadata_manager,
                                            error_holder=error_holder)
        process = [measurement_process,self._control_process]

        super().__init__(
            instance_data,
            watcher,
            process,
            interpreter,
            metadata_manager=metadata_manager,
            error_holder=error_holder,
            experiment_timeout=experiment_timeout,
        )


    def start(self):
        # Send start message
        start_topic = self._metadata_manager.experiment.start
        self._control_process.process_input(start_topic,{})
        time.sleep(1)
        super().start()
        time.sleep(1)
        # Send stop message
        stop_topic = self._metadata_manager.experiment.stop
        self._control_process.process_input(stop_topic,{})
from typing import Optional
from leaf.modules.process_modules.discrete_module import DiscreteProcess
from leaf.modules.phase_modules.start import StartPhase
from leaf.modules.phase_modules.stop import StopPhase
from leaf.modules.phase_modules.measure import MeasurePhase
from leaf.modules.phase_modules.initialisation import InitialisationPhase
from leaf_register.metadata import MetadataManager
from leaf.error_handler.error_holder import ErrorHolder
from leaf.modules.input_modules.external_event_watcher import ExternalEventWatcher
from leaf.modules.input_modules.event_watcher import EventWatcher
from leaf.modules.output_modules.output_module import OutputModule
from leaf.adapters.equipment_adapter import EquipmentAdapter
from leaf.adapters.equipment_adapter import AbstractInterpreter


class DiscreteExperimentAdapter(EquipmentAdapter):
    """
    Adapter that implements a discrete start-stop process workflow.

    It initializes and manages discrete phases for starting, stopping,
    measuring, and initializing equipment processes.
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
        external_watcher: ExternalEventWatcher = None,
    ):
        """
        Initialize the StartStopAdapter with its phases and processes.

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
        phase = [start_p, measure_p, stop_p, details_p]
        process = [DiscreteProcess(output, phase)]

        # Call the parent class constructor
        super().__init__(
            instance_data,
            watcher,
            output,
            process,
            interpreter,
            metadata_manager=metadata_manager,
            error_holder=error_holder,
            experiment_timeout=experiment_timeout,
            external_watcher=external_watcher
        )

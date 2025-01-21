from typing import Optional
from leaf.modules.process_modules.upload_module import UploadProcess
from leaf.modules.phase_modules.start import StartPhase
from leaf.modules.phase_modules.stop import StopPhase
from leaf.modules.phase_modules.measure import MeasurePhase
from leaf.modules.phase_modules.initialisation import InitialisationPhase
from leaf_register.metadata import MetadataManager
from leaf.error_handler.error_holder import ErrorHolder
from leaf.modules.output_modules.output_module import OutputModule
from leaf.adapters.equipment_adapter import EquipmentAdapter
from leaf.adapters.equipment_adapter import AbstractInterpreter
from leaf.modules.input_modules.file_watcher import FileWatcher

class UploadAdapter(EquipmentAdapter):
    """
    Generic Adapter for equipment which writes all data to 
    a file simultaneously. Also, for cases where a human must 
    manually move the data to a directory due to constraints by the 
    equipment.

    It initializes a UploadProcess which artifically dispatches 
    discrete events (start,measurements and stop).
    """
    def __init__(
        self,
        instance_data: dict,
        output: OutputModule,
        interpreter: AbstractInterpreter,
        watch_dir: Optional[str] = None,
        maximum_message_size: Optional[int] = 1,
        error_holder: Optional[ErrorHolder] = None,
        metadata_manager: Optional[MetadataManager] = None,
        experiment_timeout: Optional[int] = None):
        """
        Initialize the UploadAdapter with its phases and processes.

        Args:
            instance_data (dict): Data related to the equipment instance.
            output (OutputModule): The output module used to transmit data.
            interpreter (AbstractInterpreter): The interpreter for processing data.
            watch_dir (str): The directory path to watch for uploads.
            maximum_message_size (Optional[int]): The maximum size of messages in the MeasurePhase.
            error_holder (Optional[ErrorHolder]): Object to store and manage errors.
            metadata_manager (Optional[MetadataManager]): The metadata manager for equipment data.
            experiment_timeout (Optional[int]): Timeout for experiments in seconds.
        """

        watcher = FileWatcher(watch_dir,metadata_manager,
                              error_holder=error_holder)
        
        # Initialize phases
        start_p = StartPhase(metadata_manager)
        stop_p = StopPhase(metadata_manager)
        measure_p = MeasurePhase(metadata_manager, 
                                 maximum_message_size=maximum_message_size)
        details_p = InitialisationPhase(metadata_manager)

        # Combine phases into a discrete process
        phase = [start_p, measure_p, stop_p, details_p]
        process = [UploadProcess(output, phase)]

        # Call the parent class constructor
        super().__init__(instance_data,watcher,process,interpreter,
                         metadata_manager=metadata_manager,
                         error_holder=error_holder,
                         experiment_timeout=experiment_timeout)

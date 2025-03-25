import logging
import os
from typing import Optional

from leaf_register.metadata import MetadataManager

import leaf.start
from adapters.functional_adapters.opc_functional_adapter.interpreter import MBPOPCInterpreter
from leaf.adapters.core_adapters.discrete_experiment_adapter import DiscreteExperimentAdapter
from leaf.error_handler.error_holder import ErrorHolder
from leaf.modules.logger_modules.logger_utils import get_logger
from modules.input_modules.opc_watcher import OPCWatcher
from modules.output_modules.mqtt import MQTT

logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)
current_dir = os.path.dirname(os.path.abspath(__file__))
metadata_fn = os.path.join(current_dir, 'device.json')

class MBPOPCAdapter(DiscreteExperimentAdapter):
    def __init__(
        self,
        instance_data,
        output,
        interval: int = 10,
        topics: Optional[list[str]] = [],
        host: str = '10.22.196.201',
        port: int = 49580,
        maximum_message_size: Optional[int] = 100,
        error_holder: Optional[ErrorHolder] = None,
        experiment_timeout: Optional[int] = None
        ) -> None:

        if instance_data is None or instance_data == {}:
            raise ValueError("Instance data cannot be empty")

        metadata_manager = MetadataManager()
        watcher: OPCWatcher = OPCWatcher(metadata_manager=metadata_manager, topics=[], port=port, host=host)

        interpreter = MBPOPCInterpreter(metadata_manager=metadata_manager)

        super().__init__(instance_data=instance_data,
                         watcher=watcher,
                         output=output,
                         interpreter=interpreter,
                         maximum_message_size=maximum_message_size,
                         error_holder=error_holder,
                         metadata_manager=metadata_manager,
                         experiment_timeout=experiment_timeout)

        self._metadata_manager.add_equipment_data(metadata_fn)


if __name__ in {"__main__", "__mp_main__"}:
    # output = MQTT('localhost', 1883)
    # MBPOPCAdapter(instance_data='device.json', output=output)
    leaf.start.main(["-c", "example.yaml"])

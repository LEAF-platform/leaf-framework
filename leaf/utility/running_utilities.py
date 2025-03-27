import threading
import time
import logging
import inspect
from typing import Any
from leaf.modules.logger_modules.logger_utils import get_logger
from leaf_register.topic_utilities import topic_utilities
from leaf.modules.output_modules.mqtt import MQTT
from leaf.error_handler.exceptions import AdapterBuildError
from leaf.error_handler.error_holder import ErrorHolder
from leaf.adapters.equipment_adapter import EquipmentAdapter
from leaf import register

logger = get_logger(__name__, log_file="global.log",
                    error_log_file="global_error.log",
                    log_level=logging.INFO)

def handle_disabled_modules(output,timeout):
    '''
    Attemps to restart the output module when disabled.
    If if can restart, then all stored messages are outputed.
    '''
    if (not output.is_enabled() and 
        time.time() - output.get_disabled_time() > timeout):
        output.connect()
        connect_timeout_count = 0
        connect_timeout = 15
        while not output.is_connected():
            time.sleep(1)
            connect_timeout_count += 1
            if connect_timeout_count > connect_timeout:
                output.disable()
                return
        output.enable()
        thread = threading.Thread(target=output_messages,
                                args=(output,))
        thread.daemon = True
        thread.start()

def output_messages(output_module):
    '''
    Transmits any messages stored locally in the output modules.
    This is used when the MQTT client isnt able to transmit messages
    and messages have built up in fallback. 
    Then when the mqtt module can transmit, this function is used.
    '''
    for topic,message in output_module.pop_all_messages():
        output_module.transmit(topic,message)


def get_existing_ids(output_module: MQTT,
                      time_to_sleep: int = 5) -> list[str]:
    """Returns IDS of equipment already in the system."""
    topic = topic_utilities.details()
    logger.debug(f"Setting up subscription to {topic}")
    output_module.subscribe(topic)
    time.sleep(time_to_sleep)
    output_module.unsubscribe(topic)

    ids: list[str] = []
    for k, v in output_module.messages.items():
        if topic_utilities.is_instance(k, topic):
            ids.append(topic_utilities.parse_topic(k).instance_id)
    output_module.reset_messages()
    return ids

def get_output_module(config, error_holder: ErrorHolder) -> Any:
    """Finds, initialises and connects all desired output
        adapters defined within the config"""
    outputs = config["OUTPUTS"]
    output_objects = {}
    fallback_codes = set()

    for out_data in outputs:
        output_code = out_data.pop("plugin")
        fallback_code = out_data.pop("fallback", None)
        if fallback_code:
            fallback_codes.add(fallback_code)
        output_objects[output_code] = {
            "data": out_data,
            "fallback_code": fallback_code,
            "output": None
        }

    for code, out_data in output_objects.items():
        try:
            output_objects[code]["output"] = register.get_output_adapter(code)(
                fallback=None, error_holder=error_holder, **out_data["data"])
        except TypeError as ex:
            raise AdapterBuildError(f"code missing parameters ({ex.args})")

    for code, out_data in output_objects.items():
        if out_data["fallback_code"]:
            fallback_code = out_data["fallback_code"]
            if fallback_code not in output_objects:
                raise AdapterBuildError(f"Can't find output: {fallback_code}")

            output_objects[code]["output"].set_fallback(output_objects[fallback_code]["output"])

    for code, out_data in output_objects.items():
        if code not in fallback_codes:
            return out_data["output"]

    return None


def process_instance(instance: dict[str, Any],
                      output: MQTT,external_adapter=None) -> EquipmentAdapter:
    """Finds and initialises an adapter from the config."""
    equipment_code = instance["adapter"]
    instance_data = instance["data"]
    requirements = instance["requirements"]
    if "external_input" in instance:
        ei_data = instance["external_input"]
        ei_code = ei_data.pop("plugin")
        external_watcher = register.get_external_input(ei_code)
        external_watcher = external_watcher(**ei_data)
    else:
        external_watcher = None
    adapter = register.get_equipment_adapter(equipment_code,
                                             external_adapter=external_adapter)
    try:
        instance_id = instance_data["instance_id"]
    except KeyError:
        raise AdapterBuildError(f"Missing instance ID.")
    if instance_id in get_existing_ids(output):
        logger.warning(f"ID: {instance_id} is taken.")
    adapter_params = inspect.signature(adapter).parameters
    adapter_param_names = set(adapter_params.keys())
    fixed_params = {"instance_data", "output", "error_holder"}
    optional_params = {"maximum_message_size","experiment_timeout"}
    required_params = {
        name
        for name, param in adapter_params.items()
        if name not in fixed_params and
        param.default == inspect.Parameter.empty
    }
    provided_keys = set(requirements.keys())
    missing_keys = required_params - provided_keys
    unexpected_keys = provided_keys - adapter_param_names - optional_params

    if missing_keys:
        raise AdapterBuildError(
            f"Missing required keys for {equipment_code}: {missing_keys}"
        )
    if unexpected_keys:
        logger.warning(
            f"Unexpected keys provided for {equipment_code}: {unexpected_keys}"
        )
    try:
        error_holder = ErrorHolder(instance_id)
        return adapter(instance_data, output,
                       error_holder=error_holder,
                       external_watcher=external_watcher, 
                       **requirements)
    except ValueError as ex:
        raise AdapterBuildError(f"Error initializing {instance_id}: {ex}")
    

def start_all_adapters_in_threads(adapters):
    """Start each adapter in a separate thread."""
    threads = []
    for adapter in adapters:
        logger.info(f"Starting adapter: {adapter}")
        thread = threading.Thread(target=adapter.start)
        thread.daemon = True
        thread.start()
        threads.append(thread)
    return threads

def run_simulation_in_thread(adapter, **kwargs) -> threading.Thread:
    """Run the adapter's simulate function in a separate thread."""
    logger.info(f"Running simulation: {adapter}")

    def simulation() -> None:
        logger.info(
            f"Starting simulation using data {str(kwargs)}."
        )
        adapter.simulate(**kwargs)

    thread = threading.Thread(target=simulation)
    thread.daemon = True
    thread.start()
    return thread


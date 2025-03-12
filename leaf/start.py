""" LEAF: start.py """

##################################
#
#            PACKAGES
#
###################################

import argparse
import inspect
import logging
import os
import signal
import sys
import threading
import time
from typing import Any, Type

import yaml

from leaf import register
from leaf_register.topic_utilities import topic_utilities

from leaf.adapters.equipment_adapter import EquipmentAdapter
from leaf.modules.logger_modules.logger_utils import get_logger
from leaf.modules.logger_modules.logger_utils import set_log_dir

from leaf.error_handler.error_holder import ErrorHolder
from leaf.error_handler.exceptions import AdapterBuildError
from leaf.error_handler.exceptions import ClientUnreachableError
from leaf.error_handler.exceptions import SeverityLevel
from leaf.modules.output_modules.mqtt import MQTT
from leaf.adapters.equipment_adapter import EquipmentAdapter

from leaf.utility.running_utilities import handle_disabled_modules

##################################
#
#            VARIABLES
#
###################################
cache_dir = "cache"
error_log_dir = os.path.join(cache_dir,"error_logs")
set_log_dir(error_log_dir)
logger = get_logger(__name__, log_file="global.log",
                    error_log_file="global_error.log",
                    log_level=logging.INFO)

adapters: list[Any] = []
adapter_threads: list[Any] = []
output_disable_time = 500
##################################
#
#            FUNCTIONS
#
###################################

def parse_args(args=None) -> argparse.Namespace:
    """Parses commandline arguments."""
    parser = argparse.ArgumentParser(
        description="Proxy to monitor equipment and send data to the cloud."
    )
    parser.add_argument(
        "-d",
        "--delay",
        type=int,
        default=0,
        help="A delay in seconds before the proxy begins.",
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging.")
    parser.add_argument(
        "-c",
        "--config",
        type=str,
        # default="config.yaml",
        help="The configuration file to use.",
    )
    parser.add_argument(
        "--guidisable", action="store_false", help="Whether or not to disable the GUI."
    )
    parser.add_argument(
        "-p",
        "--path",
        type=str,
        help="The path to the directory of the adapter to use.",
        default=None
    )
    return parser.parse_args(args=args)


def signal_handler(signal_received, frame) -> None:
    """Handles shutting down of adapters when program is terminating."""
    logger.info("Shutting down gracefully.")
    stop_all_adapters()
    sys.exit(0)

def substitute_env_vars(config: Any):
    """Recursively replace placeholders in a (yaml) dictionary with environment variables."""
    if isinstance(config, dict):
        return {key: substitute_env_vars(value) for key, value in config.items()}
    elif isinstance(config, list):
        return [substitute_env_vars(item) for item in config]
    elif isinstance(config, str):
        # Replace placeholders that look like $VAR_NAME with actual env vars
        for var, value in os.environ.items():
            placeholder = f"${var}"
            if placeholder in config:
                logger.info(f"Replacing {placeholder} with its environment value")
                config = config.replace(placeholder, value)
        return config
    return

def stop_all_adapters() -> None:
    """Stop all adapters gracefully."""
    logger.info("Stopping all adapters.")
    for adapter in adapters:
        try:
            adapter.stop()
            logger.info(f"Adapter for {adapter} stopped successfully.")
        except Exception as e:
            logging.error(f"Error stopping adapter: {e}")
    
    for thread in adapter_threads:
        thread.join()


def _start_all_adapters_in_threads(adapters):
    """Start each adapter in a separate thread."""
    threads = []
    for adapter in adapters:
        logger.info(f"Starting adapter: {adapter}")
        thread = threading.Thread(target=adapter.start)
        thread.daemon = True
        thread.start()
        threads.append(thread)
    return threads


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def _get_existing_ids(output_module: MQTT,
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


def _get_output_module(config, error_holder: ErrorHolder) -> Any:
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


def _process_instance(instance: dict[str, Any],
                      output: MQTT,external_adapter=None) -> EquipmentAdapter:
    """Finds and initialises an adapter from the config."""
    equipment_code = instance["adapter"]
    instance_data = instance["data"]
    requirements = instance["requirements"]
    adapter = register.get_equipment_adapter(equipment_code,
                                             external_adapter=external_adapter)
    try:
        instance_id = instance_data["instance_id"]
    except KeyError:
        raise AdapterBuildError(f"Missing instance ID.")
    if instance_id in _get_existing_ids(output):
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
                       error_holder=error_holder, **requirements)
    except ValueError as ex:
        raise AdapterBuildError(f"Error initializing {instance_id}: {ex}")


def _run_simulation_in_thread(adapter, **kwargs) -> threading.Thread:
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


def handle_exception(exc_type: Type[BaseException], exc_value, 
                     exc_traceback) -> None:
    """Handle uncaught exceptions."""
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.error("Uncaught exception", exc_info=(exc_type, exc_value,
                                                  exc_traceback))
    stop_all_adapters()



def error_shutdown(error,output,unhandled_errors=None):
    logger.error(f"Critical error encountered: {error}. Shutting down.",
                 exc_info=error)
    # Any unhandled errors are outputed before failure.
    for adapter in adapters:
        if unhandled_errors is not None:
            adapter.transmit_errors(unhandled_errors)
            time.sleep(0.1)
        adapter.transmit_errors()
        time.sleep(0.1)
    stop_all_adapters()
    time.sleep(5)
    output.disconnect()


def run_adapters(equipment_instances, output, error_handler,
                 external_adapter=None) -> None:
    """Function to find and run a set of adapters defined within the config."""
    global adapter_threads
    cooldown_period_error = 5
    max_warning_retries = 2
    client_warning_retry_count = 0
    cooldown_period_warning = 1
    try:
        # Initialize and start all adapters
        for equipment_instance in equipment_instances:
            simulated = None
            equipment_instance = equipment_instance["equipment"]

            if "simulation" in equipment_instance:
                simulated = equipment_instance.pop("simulation")

            adapter = _process_instance(equipment_instance, output,
                                        external_adapter=external_adapter)
            if adapter is None:
                continue

            adapters.append(adapter)
            instance_id = equipment_instance["data"]["instance_id"]

            if simulated is not None:
                if not hasattr(adapter, "simulate"):
                    raise AdapterBuildError(f"Adapter does not support simulation.")

                logger.info(f"Simulator started for instance {instance_id}.")
                thread = _run_simulation_in_thread(adapter, **simulated)
                adapter_threads.append(thread)
            else:
                logger.info(f"Proxy started for instance {instance_id}.")
                thread = _start_all_adapters_in_threads([adapter])[0]
                adapter_threads.append(thread)

        while True:
            time.sleep(1)
            if all(not thread.is_alive() for thread in adapter_threads):
                logger.info("All adapters have stopped.")
                break
            
            cur_errors = error_handler.get_unseen_errors()
            # Do a double iteration because want to ensure all errors are transmited.
            # May be case that an error causes a change meaning 
            # subsequent errors arent iterated and handled.
            for adapter in adapters:
                adapter.transmit_errors(cur_errors)
                time.sleep(0.1)

            for error,tb in cur_errors:                    
                if error.severity == SeverityLevel.CRITICAL:
                    return error_shutdown(error,output,
                                          error_handler.get_unseen_errors())

                elif error.severity == SeverityLevel.ERROR:
                    logger.error(
                        f"Error, resetting adapters: {error}",
                        exc_info=error,
                    )
                    error_shutdown(error,output)
                    time.sleep(cooldown_period_error)
                    output.connect()
                    adapter_threads = _start_all_adapters_in_threads(adapters)
                    
                elif error.severity == SeverityLevel.WARNING:
                    if isinstance(error, ClientUnreachableError):
                        logger.warning(
                            f"Client error, trying to reconnect (attempt {client_warning_retry_count + 1}): {error}",
                            exc_info=error,
                        )
                        # Retry mechanism based on cumulative warnings
                        if output.is_enabled():
                            if client_warning_retry_count >= max_warning_retries:
                                logger.error(
                                    f"Disabling client {output.__class__.__name__}.",
                                    exc_info=error,
                                )
                                output.disable()
                                client_warning_retry_count = 0
                            else:
                                client_warning_retry_count += 1
                                output.disconnect()
                                time.sleep(cooldown_period_warning)
                                output.connect()
                    else:
                        logger.warning(f"Warning encountered: {error}", 
                                       exc_info=error)

                elif error.severity == SeverityLevel.INFO:
                    logger.info(f"Information error, no action: {error}",
                                exc_info=error)

            handle_disabled_modules(output,output_disable_time)

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Shutting down.")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        logger.error("An error occurred", exc_info=True)
    finally:
        stop_all_adapters()
        logger.info("Proxy stopped.")

    logger.info("All adapter threads have been stopped.")


sys.excepthook = handle_exception

def welcome_message() -> None:
    """Prints a welcome message."""
    logger.info("""\n\n ##:::::::'########::::'###::::'########:
 ##::::::: ##.....::::'## ##::: ##.....::
 ##::::::: ##::::::::'##:. ##:: ##:::::::
 ##::::::: ######:::'##:::. ##: ######:::
 ##::::::: ##...:::: #########: ##...::::
 ##::::::: ##::::::: ##.... ##: ##:::::::
 ########: ########: ##:::: ##: ##:::::::
........::........::..:::::..::..::::::::\n\n""")
    logger.info("Welcome to the LEAF Proxy.")
    logger.info("Starting the proxy.")
    logger.info("Press Ctrl+C to stop the proxy.")
    logger.info("For more information, visit leaf.systemsbiology.nl")
    logger.info("For help, use the -h flag.")
    logger.info("#" * 40)


##################################
#
#        FUNCTION: Main
#
###################################
def main(args=None) -> None:
    welcome_message()
    register.load_adapters()

    """Main function as a wrapper for all steps."""
    logger.info("Starting the proxy.")
    args = parse_args(args)

    if args.debug:
        logger.debug("Debug logging enabled.")
        logger.setLevel(logging.DEBUG)

    if args.config is None:
        logger.error("No configuration file provided (See the documentation for more details at leaf.systemsbiology.nl).")
        if os.path.isfile("config/config.yaml"):
            logger.info("An example of a config file if needed:")
            with open("config/config.yaml", "r") as file:
                logger.info("\n"+file.read())
        return

    external_adapter = args.path
    logger.debug(f"Loading configuration file: {args.config}")

    with open(args.config, "r") as file:
        config = yaml.safe_load(file)

    logger.info(f"Configuration: {args.config} loaded.")
    general_error_holder = ErrorHolder()
    output = _get_output_module(config, general_error_holder)
    run_adapters(config["EQUIPMENT_INSTANCES"], output, 
                 general_error_holder,external_adapter=external_adapter)


if __name__ == "__main__":
    main()

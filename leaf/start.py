""" LEAF: start.py """

##################################
#
#            PACKAGES
#
###################################

import os
import threading
import time
import logging
import argparse
from typing import Any

import yaml
import signal
import sys
import inspect

from leaf import register
from leaf.metadata_manager.metadata import MetadataManager

from leaf.modules.logger_modules.logger_utils import get_logger
from leaf.modules.logger_modules.logger_utils import set_log_dir

from leaf.error_handler.error_holder import ErrorHolder
from leaf.error_handler.exceptions import AdapterBuildError
from leaf.error_handler.exceptions import ClientUnreachableError
from leaf.error_handler.exceptions import LEAFError
from leaf.error_handler.exceptions import SeverityLevel

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

##################################
#
#            FUNCTIONS
#
###################################

def parse_args() -> argparse.Namespace:
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
    return parser.parse_args()


def signal_handler(signal_received, frame) -> None:
    """Handles shutting down of adapters when program is terminating."""
    logging.info("Shutting down gracefully.")
    stop_all_adapters()
    sys.exit(0)


def stop_all_adapters() -> None:
    """Stop all adapters gracefully."""
    logging.info("Stopping all adapters.")
    for adapter in adapters:
        try:
            adapter.stop()
            logging.info(f"Adapter for {adapter} stopped successfully.")
        except Exception as e:
            logging.error(f"Error stopping adapter: {e}")


def _start_all_adapters_in_threads(adapters):
    """Start each adapter in a separate thread."""
    adapter_threads = []
    for adapter in adapters:
        logger.info(f"Starting adapter: {adapter}")
        thread = threading.Thread(target=adapter.start)
        thread.daemon = True
        thread.start()
        adapter_threads.append(thread)
    return adapter_threads


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def _get_existing_ids(output_module, metadata_manager, time_to_sleep=2):
    """Returns IDS of equipment already in the system."""
    topic = metadata_manager.details()
    logging.debug(f"Setting up subscription to {topic}")
    output_module.subscribe(topic)
    time.sleep(time_to_sleep)
    output_module.unsubscribe(topic)

    ids: list[str] = []
    for k, v in output_module.messages.items():
        if metadata_manager.is_called(k, topic):
            ids.append(metadata_manager.get_instance_id(k))
    output_module.reset_messages()
    return ids


def _get_output_module(config, error_holder):
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
        output_objects[output_code] = {"data": out_data, 
                                       "fallback_code": fallback_code}

    for code, out_data in output_objects.items():
        fallback = None
        if out_data["fallback_code"]:
            try:
                fallback = output_objects[out_data["fallback_code"]].get("output")
            except KeyError:
                raise AdapterBuildError(
                    f'Cant find output: {out_data["fallback_code"]}'
                )
        try:
            output_obj = register.get_output_adapter(code)(
                fallback=fallback, error_holder=error_holder, **out_data["data"]
            )
        except TypeError as ex:
            raise AdapterBuildError(f"code missing params ({ex.args})")
        output_objects[code]["output"] = output_obj

    for code, out_data in output_objects.items():
        if code not in fallback_codes:
            return out_data["output"]
    return None


def _process_instance(instance, output):
    """Finds and initialises an adapter from the config."""
    equipment_code = instance["adapter"]
    instance_data = instance["data"]
    requirements = instance["requirements"]
    adapter = register.get_equipment_adapter(equipment_code)
    manager = MetadataManager()
    try:
        instance_id = instance_data["instance_id"]
    except KeyError:
        raise AdapterBuildError(f"Missing instance ID.")
    if instance_id in _get_existing_ids(output, manager):
        logger.warning(f"ID: {instance_id} is taken.")
    adapter_params = inspect.signature(adapter).parameters
    adapter_param_names = set(adapter_params.keys())
    fixed_params = {"instance_data", "output", "error_holder"}
    optional_params = {"maximum_message_size"}
    required_params = {
        name
        for name, param in adapter_params.items()
        if name not in fixed_params and param.default == inspect.Parameter.empty
    }
    provided_keys = set(requirements.keys())
    missing_keys = required_params - provided_keys
    unexpected_keys = provided_keys - adapter_param_names - optional_params

    if missing_keys:
        raise AdapterBuildError(
            f"Missing required keys for {equipment_code}: {missing_keys}"
        )
    if unexpected_keys:
        logging.warning(
            f"Unexpected keys provided for {equipment_code}: {unexpected_keys}"
        )
    try:
        error_holder = ErrorHolder(instance_id)
        return adapter(instance_data, output, error_holder=error_holder, **requirements)
    except ValueError as ex:
        raise AdapterBuildError(f"Error initializing {instance_id}: {ex}")


def _run_simulation_in_thread(adapter, filename, interval):
    """Run the adapter's simulate function in a separate thread."""
    logger.info(f"Running simulation: {adapter}")

    def simulation():
        logging.info(
            f"Starting simulation using file {filename} with interval {interval}."
        )
        adapter.simulate(filename, interval)

    thread = threading.Thread(target=simulation)
    thread.daemon = True
    thread.start()
    return thread


def handle_exception(exc_type, exc_value, exc_traceback):
    """Handle uncaught exceptions."""
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logging.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))
    stop_all_adapters()


def run_adapters(equipment_instances, output, error_handler):
    """Function to find and run a set of adapters defined within the config."""
    adapter_threads = []
    max_error_retries = 3
    error_retry_count = 0
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

            adapter = _process_instance(equipment_instance, output)
            if adapter is None:
                continue

            adapters.append(adapter)
            instance_id = equipment_instance["data"]["instance_id"]

            if simulated is not None:
                if not hasattr(adapter, "simulate"):
                    raise AdapterBuildError(f"Adapter does not support simulation.")

                logging.info(f"Simulator started for instance {instance_id}.")
                if not os.path.isfile(simulated["filename"]):
                    raise AdapterBuildError(f'{simulated["filename"]} doesn\'t exist')

                thread = _run_simulation_in_thread(
                    adapter, simulated["filename"], simulated["interval"]
                )
                adapter_threads.append(thread)
            else:
                logging.info(f"Proxy started for instance {instance_id}.")
                thread = _start_all_adapters_in_threads([adapter])[0]
                adapter_threads.append(thread)

        while True:
            time.sleep(1)
            if all(not thread.is_alive() for thread in adapter_threads):
                logger.info("All adapters have stopped.")
                break
            for error, tb in error_handler.get_unseen_errors():
                if not isinstance(error, LEAFError):
                    logger.error(
                        f"{error} - only LEAF errors should be added to error holder",
                        exc_info=error,
                    )
                    output.disconnect()
                    stop_all_adapters()
                    return
                
                for adapter in adapters:
                    adapter.transmit_error(error)
                if error.severity == SeverityLevel.CRITICAL:
                    logger.error(
                        f"Critical error encountered: {error}. Shutting down.",
                        exc_info=error,
                    )
                    output.disconnect()
                    stop_all_adapters()
                    return

                elif error.severity == SeverityLevel.ERROR:
                    # Only retry if below max retries
                    if error_retry_count < max_error_retries:
                        error_retry_count += 1
                        logger.error(
                            f"Error, resetting adapters (attempt {error_retry_count}): {error}",
                            exc_info=error,
                        )
                        stop_all_adapters()
                        output.disconnect()
                        time.sleep(cooldown_period_error)
                        output.connect()
                        adapter_threads = _start_all_adapters_in_threads(adapters)
                    else:
                        logger.error(
                            f"Exceeded max retries, shutting down.", exc_info=error
                        )
                        output.disconnect()
                        stop_all_adapters()
                        return

                elif error.severity == SeverityLevel.WARNING:
                    if isinstance(error, ClientUnreachableError):
                        logger.warning(
                            f"Client error, trying to reconnect (attempt {client_warning_retry_count + 1}): {error}",
                            exc_info=error,
                        )
                        # Retry mechanism based on cumulative warnings
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

    except KeyboardInterrupt:
        logging.info("Keyboard interrupt received. Shutting down.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    finally:
        stop_all_adapters()
        logging.info("Proxy stopped.")

    for thread in adapter_threads:
        thread.join()
    logging.info("All adapter threads have been stopped.")


sys.excepthook = handle_exception


##################################
#
#        FUNCTION: Main
#
###################################
def main():
    """Main function as a wrapper for all steps."""
    logging.info("Starting the proxy.")
    args = parse_args()

    if args.config is None:
        logging.error("No configuration file provided (See the documentation for more details).")
        return

    if args.debug:
        logging.debug("Debug logging enabled.")

    logging.debug(f"Loading configuration file: {args.config}")

    with open(args.config, "r") as file:
        config = yaml.safe_load(file)

    logging.info(f"Configuration: {args.config} loaded.")
    general_error_holder = ErrorHolder()
    output = _get_output_module(config, general_error_holder)
    run_adapters(config["EQUIPMENT_INSTANCES"], output, 
                 general_error_holder)


if __name__ == "__main__":
    main()

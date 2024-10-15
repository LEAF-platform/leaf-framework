""" LEAF: start.py

    VERSION:
    
    RATIONALE:


    REQUIREMENTS:
        setuptools~=75.1.0
        paho-mqtt~=2.1.0
        redis~=5.0.0b2
        aioredis~=1.3.1
        influxobject~=0.0.1
        pyyaml~=6.0.2
        poetry~=1.8.3
        watchdog~=2.1.6
        # mypy~=0.910.0
        pandas~=2.2.3


    FUNCTIONS:
        parse_args
        signal_handler, 
        stop_all_adapters,
        _get_existing_ids, 
        _get_output_module, 
        _process_instance, 
        _start_adapter_in_thread, 
        _run_simulation_in_thread, 
        handle_exception, main

"""

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
import yaml
import signal
import sys

import register as register
from core.metadata_manager.metadata import MetadataManager

from core.modules.logger_modules.logger_utils import get_logger


##################################
#
#            VARIABLES
#
###################################
logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)

adapters = []



##################################
#
#            FUNCTIONS
#
###################################
def parse_args() -> argparse.Namespace:
    """ FUNC: parse arguments

        RATIONALE:
            generate arguments for main

        INPUT:
            None

        OUTPUT:
            Command line arguments:
                --delay
                --debug
                --config
                --guidisable
    
    """
    parser = argparse.ArgumentParser(description="Proxy to monitor equipment and send data to the cloud." )

    parser.add_argument(   "-d",
                            "--delay",
                            type=int,
                            default=0,
                            help="A delay in seconds before the proxy begins.",
                        )

    parser.add_argument("--debug", 
                        action="store_true", 
                        help="Enable debug logging.",
                        )

    parser.add_argument(    "-c",
                            "--config",
                            type=str,
                            default="config.yaml",
                            help="The configuration file to use.",
                        )

    parser.add_argument(    "--guidisable",
                            action="store_false", 
                            help="Whether or not to disable the GUI.",
                        )
    
    return parser.parse_args()


def signal_handler(signal_received, frame) -> None:
    """ FUNC: Signal handler

        RATIONALE:
            Handle signals

        INPUT:
            signal_received =
            frame =

        OUTPUT:
            None,
            Stop all adapters

    """
    logging.info("Shutting down gracefully.")

    stop_all_adapters()

    sys.exit(0)


def stop_all_adapters() -> None:
    """ FUNC: Stop all adapters
 
        RATIONALE:
            Function to stop all adapters

        INPUT:
            None

        OUTPUT:   
            None,
            stop all adapters
    
    """
    logging.info("Shutting down all adapters.")

    for adapter in adapters:

        try:
            adapter.stop()
            logging.info(f"Adapter for {adapter} stopped successfully.")

        except Exception as e:
            logging.error(f"Error stopping adapter: {e}")


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def _get_existing_ids(output_module, metadata_manager, time_to_sleep = 2):
    """ FUNC: Get existing IDs

        RATIONALE:
            Get all existing IDs

        INPUT:
            output_module =
            metadata_manager = 
            time_to_sleep = time to sleep, default is 2

        OUTPUT:    
            ids =
            
    """
    topic = metadata_manager.details()
    output_module.subscribe(topic)

    time.sleep(time_to_sleep)
    
    output_module.unsubscribe(topic)

    ids: list[str] = []

    for k, v in output_module.messages.items():

        if metadata_manager.is_called(k, topic):
            ids.append(metadata_manager.get_instance_id(k))

    output_module.reset_messages()

    return ids


def _get_output_module(config):
    """ FUNC:

        INPUT:
            config =
    
    """
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
            fallback = output_objects[out_data["fallback_code"]].get("output")

        output_obj = register.get_output_adapter(code)(fallback=fallback, **out_data["data"])

        output_objects[code]["output"] = output_obj


    for code, out_data in output_objects.items():

        if code not in fallback_codes:

            return out_data["output"]

    return None


def _process_instance(instance, output):
    """ FUNC: Processes a single equipment instance.
    
    
    
    """
    equipment_code = instance["adapter"]
    data = instance["data"]
    requirements = instance["requirements"]
    adapter = register.get_equipment_adapter(equipment_code)
    manager = MetadataManager()

    if data["instance_id"] in _get_existing_ids(output, manager):

        logger.warning(f'ID: {data["instance_id"]} is taken.')

    try:
        equipment_adapter = adapter(data, output, **requirements)

    except ValueError as ex:
        logging.error(f"Error initialising {data['instance_id']}: {ex}")

        return None

    return equipment_adapter


def _start_adapter_in_thread(adapter):
    """ FUNC: Run the adapter's start function in a separate thread.
    
    
    """

    logger.info(f"Running adapter: {adapter}")
    thread = threading.Thread(target=adapter.start)
    thread.daemon = True
    thread.start()
    return thread


def _run_simulation_in_thread(adapter, filename, interval):
    """ FUNC: Run the adapter's simulate function in a separate thread.
    
    
    """
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
    """
    
    """

    if issubclass(exc_type, KeyboardInterrupt):

        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    
    logging.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))

    stop_all_adapters()


sys.excepthook = handle_exception


##################################
#
#        FUNCTION: Main
#
###################################
def main(time_to_sleep = 1):
    """ FUNC: Main 

        RATIONALE:
            wrapper for all the functions

        INPUT:
            time_to_sleep = time to sleep, default is 1

        OUTPUT:
            None,
    
    """

    #------------------------
    # Step 1: Load and parse configuration arguments
    logging.info("Starting the proxy.")

    # Step 1a: Parse Arguments
    args = parse_args()

    # Step 1b: Check if debug mode is enabled
    if args.debug:
        logging.debug("Debug logging enabled.")
        # logging.basicConfig(level=logging.DEBUG)

    # Step 1c: Load configuration file
    logging.debug(f"Loading configuration file: {args.config}")
    
    with open(args.config, "r") as file:
        config = yaml.safe_load(file)

    logging.info(f"Configuration: {args.config} loaded.")

    # Step 1d: Assign output modules
    output = _get_output_module(config)

    # Step 1e: Get adapters
    adapter_threads = []

    try:
        for equipment_instance in config["EQUIPMENT_INSTANCES"]:

            simulated = None
            equipment_instance = equipment_instance["equipment"]

            if "simulation" in equipment_instance:
                simulated = equipment_instance.pop("simulation")

            adapter = _process_instance(equipment_instance, output)

            if adapter is None:
                continue

            adapters.append(adapter)
            equipment_id = equipment_instance["adapter"]
            instance_id = equipment_instance["data"]["instance_id"]

            if simulated is not None:
                if not hasattr(adapter, "simulate"):
                    raise NotImplementedError( f"Adapter {equipment_id} does not support simulation." )
                
                logging.info(f"Simulator started for instance {instance_id}.")

                if not os.path.isfile(simulated["filename"]):
                    raise ValueError(f'{simulated["filename"]} doesn\'t exist')

                thread = _run_simulation_in_thread(adapter, simulated["filename"], simulated["interval"])

                adapter_threads.append(thread)

            else:
                logging.info(f"Proxy started for instance {instance_id}.")

                thread = _start_adapter_in_thread(adapter)
                adapter_threads.append(thread)

        while True:

            time.sleep(time_to_sleep)

    except KeyboardInterrupt:
        logging.info("Keyboard interrupt received.")

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        e.with_traceback()

    finally:
        stop_all_adapters()
        logging.info("Proxy stopped.")

    for thread in adapter_threads:

        thread.join()  # Wait for each thread to finish
        
    logging.info("All adapter threads have been stopped.")


if __name__ == "__main__":

    main()

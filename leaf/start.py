""" LEAF: start.py """

##################################
#
#            PACKAGES
#
###################################

import argparse
import logging
import os
import signal
import sys
import time
from typing import Any, Type
import threading
import time

import yaml

from leaf import register
from leaf.modules.logger_modules.logger_utils import get_logger
from leaf.modules.logger_modules.logger_utils import set_log_dir
from leaf.error_handler.error_holder import ErrorHolder
from leaf.error_handler.exceptions import AdapterBuildError
from leaf.error_handler.exceptions import ClientUnreachableError
from leaf.error_handler.exceptions import SeverityLevel
from leaf.utility.running_utilities import handle_disabled_modules
from leaf.utility.running_utilities import get_output_module
from leaf.utility.running_utilities import process_instance
from leaf.utility.running_utilities import start_all_adapters_in_threads
from leaf.utility.running_utilities import run_simulation_in_thread
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

# Global variables for the app
global_output = None
global_error_handler = None
global_config = None
# global_external_adapter = None
global_gui = None
global_args = None

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
        "--port",
        type=int,
        default=8080,
        help="The port to run the NiceGUI web interface on.",
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

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def stop_all_adapters() -> None:
    """Stop all adapters gracefully."""
    logger.info("Stopping all adapters.")
    adapter_timeout = 10
    count = 0
    while len(adapters) == 0:
        time.sleep(0.5)
        count += 1
        if count >= adapter_timeout:
            raise AdapterBuildError(f"Cant stop adapter, likely because it hasn't started fully before shutdown.")
    for adapter in adapters:
        if adapter.is_running():
            adapter.withdraw()
    for adapter in adapters:
        try:
            if adapter.is_running():
                adapter.stop()
                logger.info(f"Adapter for {adapter} stopped successfully.")
        except Exception as e:
            logging.error(f"Error stopping adapter: {e}")
    
    for thread in adapter_threads:
        thread.join()

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

            adapter = process_instance(equipment_instance, output,
                                        external_adapter=external_adapter)
            if adapter is None:
                continue

            adapters.append(adapter)
            instance_id = equipment_instance["data"]["instance_id"]

            if simulated is not None:
                if not hasattr(adapter, "simulate"):
                    raise AdapterBuildError(f"Adapter does not support simulation.")

                logger.info(f"Simulator started for instance {instance_id}.")
                thread = run_simulation_in_thread(adapter, **simulated)
                adapter_threads.append(thread)
            else:
                logger.info(f"Proxy started for instance {instance_id}.")
                thread = start_all_adapters_in_threads([adapter])[0]
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
                    adapter_threads = start_all_adapters_in_threads(adapters)
                    
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
    global global_gui, global_output, global_error_handler, global_config, global_external_adapter
    welcome_message()

    # register.load_adapters()

    """Main function as a wrapper for all steps."""
    logger.info("Starting the proxy.")
    args = parse_args(args)
    global_args = args

    if args.debug:
        logger.debug("Debug logging enabled.")
        logger.setLevel(logging.DEBUG)

    # Create configuration folder to store the config file in
    # Obtain script directory
    script_dir = os.path.dirname(os.path.realpath(__file__))
    # Create config directory
    if not os.path.exists(os.path.join(script_dir, "config")):
        os.makedirs(os.path.join(script_dir, "config"))
    # Check if a config file is provided
    if args.config is not None:
        # Check if the config file exists
        if os.path.exists(args.config):
            # Copy to the config directory
            with open(os.path.join(script_dir, "config", "configuration.yaml"), "w") as f:
                with open(args.config, "r") as f2:
                    f.write(f2.read())
    # Fixed path to the configuration file
    args.config = os.path.join(script_dir, "config", "configuration.yaml")

    # external_adapter = args.path
    # logger.debug(f"Loading configuration file: {args.config}")
    #
    # with open(args.config, "r") as file:
    #     config = yaml.safe_load(file)
    #
    # logger.info(f"Configuration: {args.config} loaded.")
    # general_error_holder = ErrorHolder()
    # output = get_output_module(config, general_error_holder)
    # run_adapters(config["EQUIPMENT_INSTANCES"], output,
    #              general_error_holder,external_adapter=external_adapter)

    # If GUI is enabled, run NiceGUI as the main application
    import threading
    import time
    logger.info(f"Starting NiceGUI web interface on port {args.port}")
    # Create GUI instance
    from leaf.interface.main import create_gui
    global_gui = create_gui(args.port)
    # Set global variables
    global_gui.global_args = global_args
    global_gui.global_config = global_config
    # Register callbacks for GUI to use
    global_gui.register_callbacks(
        start_adapters_func=run_adapters,
        stop_adapters_func=stop_all_adapters
    )

    # Function to run background tasks (adapter setup)
    def run_background_tasks():
        global_external_adapter = args.path
        logger.debug(f"Loading configuration file: {args.config}")

        # Load the configuration file
        if args.config is None or not os.path.exists(args.config):
            logger.error(
                "No configuration file provided (See the documentation for more details at leaf.systemsbiology.nl).")
            global_config: str = "No configuration file provided."
            config = None
        else:
            with open(args.config, "r") as file:
                config = yaml.safe_load(file)
                global_config: str = yaml.dump(config, indent=4)

            logger.info(f"Configuration: {args.config} loaded.")
            logger.info(f"\n{global_config}\n")

            general_error_holder = ErrorHolder()
            global_error_handler = general_error_holder
            output = get_output_module(config, general_error_holder)
            global_output = output
            run_adapters(config["EQUIPMENT_INSTANCES"], output, general_error_holder, external_adapter=None)

    # Start the background tasks (adapter setup) in a separate thread
    background_thread = threading.Thread(target=run_background_tasks, daemon=True)
    background_thread.start()

    # Run the GUI in the main thread
    global_gui.run()



if __name__ in {"__main__", "__mp_main__"}:
    main()

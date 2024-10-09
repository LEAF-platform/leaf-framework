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

DEBUG = False

# Configure logging
logger = logging.getLogger(__name__)
logger.propagate = False  # Prevent the logger from passing logs to the parent/root logger
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler("app.log")
file_handler.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

adapters = []


def parse_args() -> argparse.Namespace:
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

    parser.add_argument("--debug", action="store_true", 
                        help="Enable debug logging.")

    parser.add_argument(
        "-c",
        "--config",
        type=str,
        default="config.yaml",
        help="The configuration file to use.",
    )

    return parser.parse_args()


def signal_handler(signal_received, frame):
    logging.info("Shutting down gracefully.")
    stop_all_adapters()
    sys.exit(0)


def stop_all_adapters():
    logging.info("Shutting down all adapters.")
    for adapter in adapters:
        try:
            adapter.stop()
            logging.info(f"Adapter for {adapter} stopped successfully.")
        except Exception as e:
            logging.error(f"Error stopping adapter: {e}")


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def _get_existing_ids(output_module, metadata_manager):
    topic = metadata_manager.details()
    output_module.subscribe(topic)
    time.sleep(2)
    output_module.unsubscribe(topic)
    ids = []
    for k, v in output_module.messages.items():
        if metadata_manager.is_called(k, topic):
            ids.append(metadata_manager.get_instance_id(k))
    output_module.reset_messages()
    return ids


def _get_output_module(config):
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
        output_obj = register.get_output_adapter(code)(
            fallback=fallback, **out_data["data"]
        )
        output_objects[code]["output"] = output_obj

    for code, out_data in output_objects.items():
        if code not in fallback_codes:
            return out_data["output"]

    return None


def _process_instance(instance, output):
    """Processes a single equipment instance."""
    equipment_code = instance["adapter"]
    data = instance["data"]
    requirements = instance["requirements"]
    adapter = register.get_equipment_adapter(equipment_code)
    manager = MetadataManager()
    if data["instance_id"] in _get_existing_ids(output, manager):
        raise ValueError(f'ID: {data["instance_id"]} is taken.')

    try:
        equipment_adapter = adapter(data, output, **requirements)
    except ValueError as ex:
        logging.error(f"Error initialising {data['instance_id']}: {ex}")
        return None

    return equipment_adapter


def _start_adapter_in_thread(adapter):
    """Run the adapter's start function in a separate thread."""
    print(f"Running adapter: {adapter}")
    thread = threading.Thread(target=adapter.start)
    thread.daemon = True
    thread.start()
    return thread


def _run_simulation_in_thread(adapter, filename, interval):
    """Run the adapter's simulate function in a separate thread."""
    print(f"Running simulation: {adapter}")

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
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logging.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))
    stop_all_adapters()


sys.excepthook = handle_exception


def main():
    logging.info("Starting the proxy.")
    args = parse_args()
    if args.debug:
        logging.debug("Debug logging enabled.")
        logging.basicConfig(level=logging.DEBUG)

    logging.debug(f"Loading configuration file: {args.config}")
    with open(args.config, "r") as file:
        config = yaml.safe_load(file)
    logging.info(f"Configuration: {args.config} loaded.")

    output = _get_output_module(config)

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
                    raise NotImplementedError(
                        f"Adapter {equipment_id} does not support simulation."
                    )
                logging.info(f"Simulator started for instance {instance_id}.")
                if not os.path.isfile(simulated["filename"]):
                    raise ValueError(f'{simulated["filename"]} doesn\'t exist')

                thread = _run_simulation_in_thread(
                    adapter, simulated["filename"], simulated["interval"]
                )
                adapter_threads.append(thread)
            else:
                logging.info(f"Proxy started for instance {instance_id}.")
                thread = _start_adapter_in_thread(adapter)
                adapter_threads.append(thread)

        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        logging.info("Keyboard interrupt received.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    finally:
        stop_all_adapters()
        logging.info("Proxy stopped.")

    for thread in adapter_threads:
        thread.join()  # Wait for each thread to finish
    logging.info("All adapter threads have been stopped.")


if __name__ == "__main__":
    main()

import argparse
import logging

import yaml

import register as register

DEBUG = False

logging.basicConfig(level=logging.INFO)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Proxy to monitor equipment and send data to the cloud."
    )
    # parser.add_argument(
    #     '--simulated',
    #     type=str,
    #     default=None,
    #     help='Run in simulated mode and provide file to take data from.'
    # )
    # This should be at configuration level as each adapter can have different config
    # parser.add_argument(
    #     '-s', '--seconds',
    #     type=int,
    #     default=None,
    #     help='Number of seconds between changes (optional for Simulated mode).'
    # )
    parser.add_argument(
        '-d', '--delay', 
        type=int, 
        default=0,
        help='A delay in seconds before the proxy begins.' # TODO is this needed?
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging.'
    )

    parser.add_argument(
        "-c", "--config",
        type=str,
        default="config.yaml",
        help="The configuration file to use."
    )

    return parser.parse_args()

def _get_output_module(config):
    outputs = config["OUTPUTS"]
    output_objects = {}
    fallback_codes = set()
    for out_data in outputs:
        logging.debug(f"Output data: {out_data}")
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
        output_obj = register.get_output_adapter(code)(fallback=fallback,
                                                        **out_data["data"])
        output_objects[code]["output"] = output_obj
    for code, out_data in output_objects.items():
        if code not in fallback_codes:
            return out_data["output"]
    return None


def main():
    logging.info("Starting the proxy.")
    # Parse command line arguments
    args = parse_args()
    # Set up logging
    if args.debug:
        logging.debug("Debug logging enabled.")
        logging.basicConfig(level=logging.DEBUG)
    # Load the configuration file
    logging.debug(f"Loading configuration file: {args.config}")
    with open(args.config, 'r') as file:
        config = yaml.safe_load(file)
    logging.info(f"Configuration: {args.config} loaded.")

    # delay = 0
    # if args.delay is not None:

    # Obtain the delay from the command line arguments, TODO is this needed?
    logging.debug(f"Delay: {args.delay} of type {type(args.delay)}")
    # delay = int(args.delay)

    # Get the output module, multiple outputs are supported
    output = _get_output_module(config)

    # Get the equipment module TODO (AKA adapters?)
    equipment_requirements = config["EQUIPMENT"]
    # TODO what is equipment code?
    equipment_code = equipment_requirements.pop("code")
    # Adapters for the different equipment in your laboratory
    adapter = register.get_equipment_adapter(equipment_code)
    instance_data = config["EQUIPMENT_DATA"]

    # Checking if the unique ID is already taken?
    if instance_data["instance_id"] in output.get_existing_ids():
        raise ValueError(f'ID: {instance_data["instance_id"]} is taken.')
    try:
        adapter = adapter(instance_data,output, **equipment_requirements)
    except ValueError as ex:
        print("Error:")
        print(ex)
        exit(-1) # TODO Exit with error code -1 is that correct?

    # TODO This should be moved to the adapter
    #     if not hasattr(adapter,"simulate"):
    #         raise NotImplementedError(f'Adapter {equipment_code} doesnt have a simulator.')
    #     print("Simulator started.")
    #     adapter.simulate(args.simulated,args.seconds,delay)
    # else:
    #     print("Proxy started.")
    adapter.start()

if __name__ == "__main__":
    main()
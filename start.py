import argparse
import yaml
import register as register

def parse_args():
    parser = argparse.ArgumentParser(
        description="Proxy to monitor a supported bioreactor"
    )
    parser.add_argument(
        '--simulated', 
        type=str, 
        default=None, 
        help='Run in simulated mode and provide file to take data from.'
    )                                       
    parser.add_argument(
        '-s', '--seconds', 
        type=int, 
        default=None, 
        help='Number of seconds between changes (optional for Simulated mode).'
    )
    parser.add_argument(
        '-d', '--delay', 
        type=int, 
        default=None, 
        help='A delay in seconds before the proxy begins.'
    )
    return parser.parse_args()

def _get_output_module(config):
    outputs = config["OUTPUTS"]
    output_objects = {}
    fallback_codes = set()
    for out_data in outputs:
        output_code = out_data.pop("code")
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
    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    delay = 0
    args = parse_args()
    if args.delay is not None:
        delay = int(args.delay)
    
    output = _get_output_module(config)

    equipment_requirements = config["EQUIPMENT"]
    equipment_code = equipment_requirements.pop("code")
    adapter = register.get_equipment_adapter(equipment_code)
    instance_data = config["EQUIPMENT_DATA"]
    if instance_data["instance_id"] in output.get_existing_ids():
        raise ValueError(f'ID: {instance_data["instance_id"]} is taken.')
    try:
        adapter = adapter(instance_data,output,
                          **equipment_requirements)
    except ValueError as ex:
        print("Error:")
        print(ex)
        exit(-1)

    if args.simulated is not None:
        if not hasattr(adapter,"simulate"):
            raise NotImplementedError(f'Adapter {equipment_code} doesnt have a simulator.')
        print("Simulator started.")
        adapter.simulate(args.simulated,args.seconds,delay)
    else:
        print("Proxy started.")
        adapter.start()

if __name__ == "__main__":
    main()
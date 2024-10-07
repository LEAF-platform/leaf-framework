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

def main():
    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    delay = 0
    args = parse_args()
    print(config)
    if args.delay is not None:
        delay = int(args.delay)
    
    output_code = config["OUTPUT"].pop("code")
    output = register.get_output_adapter(output_code)
    output = output(**config["OUTPUT"])

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
        print("Simulator started.")
        adapter.simulate(args.simulated,args.seconds,delay)
    else:
        print("Proxy started.")
        adapter.start()

if __name__ == "__main__":
    main()
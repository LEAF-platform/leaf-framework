import os
import sys

import yaml

sys.path.insert(0, os.path.join(".."))
sys.path.insert(0, os.path.join("..",".."))
sys.path.insert(0, os.path.join("..","..",".."))

with open('../../test_config.yaml', 'r') as file:
    config = yaml.safe_load(file)
broker = config["OUTPUT"]["broker_address"]
port = int(config["OUTPUT"]["port"])
un = config["OUTPUT"]["username"]
pw = config["OUTPUT"]["password"]
test_file_dir = "test_dir"
test_file = os.path.join(test_file_dir,"ecoli-GFP-mCherry_inter.csv")
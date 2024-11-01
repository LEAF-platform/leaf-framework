from core.adapters.functional_adapters.minknow import interpreter

import logging
import os
import re
import unittest

import yaml

from core.adapters.functional_adapters.minknow import interpreter
from core.adapters.functional_adapters.minknow import adapter
from core.modules.logger_modules.logger_utils import get_logger
from core.modules.output_modules.mqtt import MQTT

logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)

class MinKNOWCase(unittest.TestCase):
    def setUp(self) -> None:
        logger.info("Clearing log file")
        if os.path.exists("app.log"):
            os.remove("app.log")
        # Load example.yaml
        # Current location of this script
        curr_dir: str = os.path.dirname(os.path.realpath(__file__))

        with open(curr_dir + "/../example.yaml", "r") as file:
            self._config = yaml.safe_load(file)
            logger.info(f"Config: {self._config}")
            self._token = self._config['EQUIPMENT_INSTANCES'][0]['equipment']['requirements']['token']

    def test_minknow_adapter(self) -> None:
        mqtt_output = MQTT("localhost", 1883)
        mqtt_output.transmit("test", """'{"test": "test"}""")
        curr_dir = "/".join(__file__.split("/")[:-1])
        self.instance_data: dict[str, str] = {
            "instance_id": "test_maq",
            "institute": "test_ins",
            "experiment_id": "test_exp",
        }
        logger.debug(f"Token: {self._token}")
        adap = adapter.MinKNOWAdapter(instance_data=self.instance_data, output=mqtt_output, token=self._token, write_file=None)
        adap.start()


    def test_maq_interpreter_id(self) -> None:
        self._token = "bla"
        inter = interpreter.MinKNOWInterpreter(token=self._token, host="localhost", port=9501)
        logger.debug(f"ID of interpreter: {inter.id}")



if __name__ == "__main__":
    unittest.main()

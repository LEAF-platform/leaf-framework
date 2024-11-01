import json
import logging
import os
import re
import unittest

import yaml

from core.adapters.functional_adapters.maq_observations import interpreter
from core.adapters.functional_adapters.maq_observations import adapter
from core.modules.logger_modules.logger_utils import get_logger
from core.modules.output_modules.mqtt import MQTT

logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)

class MAQCase(unittest.TestCase):
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
            token = self._config['EQUIPMENT_INSTANCES'][0]['equipment']['requirements']['token']
            if token.startswith("$"):
                token_name = re.search(r"\$\{(\w+)\}", token).group(1)
                token_value = os.getenv(token_name)
                logger.debug(f"Token: {token_value}")
                self._token = token_value
            if token_value is None:
                raise ValueError(f"No token found for {token_name}")

    def test_maq_adapter(self) -> None:
        mqtt_output = MQTT("localhost", 1883)
        mqtt_output.transmit("test", """'{"test": "test"}""")
        curr_dir = "/".join(__file__.split("/")[:-1])
        self.instance_data: dict[str, str] = {
            "instance_id": "test_maq",
            "institute": "test_ins",
            "experiment_id": "test_exp",
        }
        logger.debug(f"Token: {self._token}")
        adap = adapter.MAQAdapter(instance_data=self.instance_data, output=mqtt_output, token=self._token, write_file=None)
        adap.start()
        # inter = interpreter.MAQInterpreter(token=self._token)
        # influxobjects = inter.measurement()
        # print("stop...")


    def test_maq_interpreter_id(self) -> None:
        inter = interpreter.MAQInterpreter(token=self._token)
        logger.debug(f"ID of interpreter: {inter.id}")
        # influxobjects = inter.measurement()
        # logger.debug(f"Influx objects: {len(influxobjects)} prepared for submission")
        # print("stop...")



if __name__ == "__main__":
    unittest.main()

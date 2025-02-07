import os
import yaml
import unittest

from leaf.start import substitute_env_vars


class TestConfigWithSysVariables(unittest.TestCase):
    def test_config_with_sys_variables(self) -> None:
        # Set environment variable for test
        os.environ["MQTT_PASSWORD"] = "qhuhf92384!±q3iht"

        # Load YAML
        with open("test_config_secret.yaml", "r") as file:
            config = yaml.safe_load(file)

        # Substitute environment variables
        config = substitute_env_vars(config)

        # Assertions
        self.assertEqual(config["OUTPUTS"][0]["password"], "qhuhf92384!±q3iht")
        self.assertEqual(config["OUTPUTS"][0]["username"], "mcrowther")
        self.assertEqual(config["OUTPUTS"][0]["broker"], "localhost")

        # Optional: Print for debugging
        print(config)

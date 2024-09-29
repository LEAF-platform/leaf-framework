
# Test the config_loader.py module

import os
import unittest

from core import config_loader

class TestConfig(unittest.TestCase):
    def test_config(self):
        # Initialize the config
        config = config_loader.Config('config.ini')
        assert isinstance(config.config, config.ConfigParser)

        # Check if the config file exists
        assert os.path.exists('config.ini')

        # Check if the config file has the expected sections
        assert 'mqtt' in config.config



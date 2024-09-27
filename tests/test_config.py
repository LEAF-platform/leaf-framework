
# Test the config.py module

import os
import unittest

from core import config

class TestConfig(unittest.TestCase):
    def test_config(self):
        # Initialize the config
        config = config.Config('config.ini')
        assert isinstance(config.config, config.ConfigParser)

        # Check if the config file exists
        assert os.path.exists('config.ini')

        # Check if the config file has the expected sections
        assert 'mqtt' in config.config



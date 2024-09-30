import configparser
import logging
import os

logging.basicConfig(level=logging.INFO)


class Config:
    def __init__(self, config_file):
        # Obtain the absolute path of the config file
        config_file = os.path.abspath(config_file)
        logging.info(f"Config file: {config_file}")
        self.config = configparser.ConfigParser()
        logging.info(f"Reading config file: {config_file}")
        self.config.read(config_file)
        # Show all keys
        for section in self.config.sections():
            logging.info(f"Section: {section}")
            for key in self.config[section]:
                logging.info(f"Key: {key}, Value: {self.config[section][key]}")

    def get(self, section, key):
        logging.debug(f"Getting config value for section: {section}, key: {key}")
        if section not in self.config:
            return None
        if key not in self.config[section]:
            return None
        return self.config[section][key]

    def set(self, section, key, value):
        logging.debug(f"Setting config value for section: {section}, key: {key}, value: {value}")
        self.config[section][key] = value

    def save(self, config_file):
        logging.debug(f"Saving config file: {config_file}")
        with open(config_file, 'w') as configfile:
            self.config.write(configfile)

    def get_int(self, section, key) -> int:
        return int(self.config[section][key])

    def to_dict(self):
        return self.config._sections

    def has_option(self, param, param1):
        return self.config.has_option(param, param1)

    
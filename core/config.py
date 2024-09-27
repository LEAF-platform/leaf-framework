import configparser

class Config:
    def __init__(self, config_file):
        self.config = configparser.ConfigParser()
        self.config.read(config_file)

    def get(self, section, key):
        return self.config[section][key]

    def set(self, section, key, value):
        self.config[section][key] = value

    def save(self, config_file):
        with open(config_file, 'w') as configfile:
            self.config.write(configfile)
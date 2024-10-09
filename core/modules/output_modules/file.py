import json
import os
from core.modules.output_modules.output_module import OutputModule

class FILE(OutputModule):
    def __init__(self, filename, fallback=None):
        super().__init__(fallback=fallback)
        self.filename = filename

    def transmit(self, topic, data=None):
        if os.path.exists(self.filename):
            with open(self.filename, 'r') as f:
                try:
                    file_data = json.load(f)
                except json.JSONDecodeError:
                    file_data = {}
        else:
            file_data = {}

        if topic in file_data:
            if not isinstance(file_data[topic], list):
                file_data[topic] = [file_data[topic]]
        else:
            file_data[topic] = []
        if data is not None:
            file_data[topic].append(data)

        with open(self.filename, 'w') as f:
            json.dump(file_data, f, indent=4)

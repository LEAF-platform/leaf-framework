import redis
import logging
import json

from core.modules.output_modules.output_module import OutputModule
logging.basicConfig(level=logging.INFO)

class KEYDB(OutputModule):
    def __init__(self, host, port=6379, db=0, fallback=None):
        super().__init__(fallback=fallback)
        self.host = host
        self.port = port
        self.db = db
        self.client = None

    def connect(self):
        self.client = redis.StrictRedis(host=self.host, 
                                        port=self.port, 
                                        db=self.db)

    def transmit(self, topic, data=None):
        try:
            self.client.set(topic, data)
            logging.info(f"Transmit data to key '{topic}'")
            return True
        except redis.RedisError as e:
            if self._fallback is not None:
                self._fallback.transmit(topic,data=data)
            logging.error(f"Transmit data to key '{topic}': {str(e)}")
            return False

    def retrieve(self, key):
        try:
            message = self.client.get(key)
            if message:
                return message.decode('utf-8')
            return None
        except redis.RedisError as e:
            logging.error(f"No data for key '{key}': {str(e)}")
            return None
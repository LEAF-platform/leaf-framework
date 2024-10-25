import logging

import redis

from core.modules.output_modules.output_module import OutputModule

# logging.basicConfig(level=logging.INFO)

class KEYDB(OutputModule):
    def __init__(self, host, port=6379, db=0, fallback=None) -> None:
        super().__init__(fallback=fallback)
        self.host: str = host
        self.port: int = port
        self.db: int = db
        self.client = None

    def connect(self) -> None:
        self.client = redis.StrictRedis(host=self.host, 
                                        port=self.port, 
                                        db=self.db)

    def transmit(self, topic: str, data: str) -> bool:
        if self.client is None:
            return self._fallback.transmit(topic,data=data)
        try:
            # Should this be rpush? It could be the case 
            # multiple messages with the same topic?
            # This case as far as I can tell will overwrite data.
            self.client.set(topic, data)
            logging.info(f"Transmit data to key '{topic}'")
            return True
        except redis.RedisError as e:
            if self._fallback is not None:
                self._fallback.transmit(topic,data=data)
            logging.error(f"Transmit data to key '{topic}': {str(e)}")
            return False

    def disconnect(self) -> None:
        if self.client is not None:
            self.client = None
            logging.info("Disconnected from the Redis/KeyDB instance.")
        else:
            logging.info("Already disconnected.")

    def retrieve(self, key: str) -> str|None:
        try:
            if self.client is None:
                return None
            message = self.client.get(key)
            if message:
                return message.decode('utf-8')
            return None
        except redis.RedisError as e:
            logging.error(f"No data for key '{key}': {str(e)}")
            return None
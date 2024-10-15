from typing import Literal, Optional
from uuid import uuid4

import paho.mqtt.client as mqtt
import time
import logging
import json
from core.modules.output_modules.output_module import OutputModule

FIRST_RECONNECT_DELAY = 1
RECONNECT_RATE = 2
MAX_RECONNECT_COUNT = 12
MAX_RECONNECT_DELAY = 60

from core.modules.logger_modules.logger_utils import get_logger

logger = get_logger(__name__, log_file="app.log", log_level=logging.DEBUG)


class MQTT(OutputModule):
    def __init__(self, broker, port=1883,
                 username=None,password=None,fallback=None, 
                 clientid: Optional[str]=None, protocol="v3",
                 transport: Literal['tcp', 'websockets', 'unix']='tcp', 
                 tls: bool=False):
        super().__init__(fallback=fallback)
        self.protocol = mqtt.MQTTv5 if '5' in protocol.__str__() else mqtt.MQTTv311
        logger.debug(f"MQTT protocol: {self.protocol}")
        logger.debug(f"MQTT transport: {transport}")
        logger.debug(f"MQTT client ID: {clientid}")
        logger.debug(f"MQTT TLS: {tls}")

        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, 
                                  protocol=self.protocol, 
                                  transport=transport, 
                                  client_id=clientid)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_connect_failure = self.on_connect_failure
        self.client.on_log = self.on_log
        self.client.on_message = self.on_message
        self.session_start_time = time.time()

        if username is not None and password is not None:
            logger.debug(f"MQTT username: {username}")
            self.client.username_pw_set(username,password)
        if tls:
            logger.debug(f"MQTT TLS enabled")
            self.client.tls_set()
            self.client.tls_insecure_set(True)

        logger.debug(f"Connecting to MQTT broker at {broker}:{port}")
        self.client.connect(broker, port, 60)

        self.client.loop_start()
        self.messages = {}

    
    def transmit(self, topic,data=None,retain=False):
        if isinstance(data, (dict,list)):
            data = json.dumps(data)
        elif data is not None and not isinstance(data, str):
            data = str(data)
        elif data is None:
            logger.error(f"No data was provided")
            data = ""
        logger.debug(f"Transmitting message to {topic}: {data[:50]}")
        result = self.client.publish(topic=topic, 
                                     payload=data, 
                                     qos=0, retain=retain)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            if self._fallback is not None:
                self._fallback.transmit(topic,data=data)
            else:
                logger.error(f"Failed to send message: {result.rc}")

    def flush(self,topic):
        self.client.publish(topic=topic, 
                            payload=None, 
                            qos=0, retain=True)
        
    def on_connect(self, client, userdata, flags, rc, metadata):
        if rc != 0:
            logger.error(f"Failed to connect: {rc}")

    def on_disconnect(self, client, userdata, flags, rc, metadata):
        logger.error(f"Disconnected: {rc}")
        reconnect_count, reconnect_delay = 0, FIRST_RECONNECT_DELAY
        while reconnect_count < MAX_RECONNECT_COUNT:
            logger.info(f"Retry: {reconnect_count}")
            time.sleep(reconnect_delay)
            try:
                client.reconnect()
                logger.info(f"Reconnected")
                return
            except Exception as err:
                pass
            reconnect_delay *= RECONNECT_RATE
            reconnect_delay = min(reconnect_delay, MAX_RECONNECT_DELAY)
            reconnect_count += 1
        logger.error(f"Unable to reconnect")

    def on_connect_failure(self, client, userdata, flags, rc, metadata):
        logger.error(f"Failed to connect: {rc}")

    def on_log(self, client, userdata, paho_log_level, message):
        if paho_log_level == mqtt.LogLevel.MQTT_LOG_ERR:
            print(message, paho_log_level)

    def on_message(self, client, userdata, msg):
        payload = msg.payload.decode()
        topic = msg.topic
        if topic not in self.messages:
            self.messages[topic] = []
        self.messages[topic].append(payload)

    def reset_messages(self):
        self.messages = {}

    def subscribe(self, topic):
        self.client.subscribe(topic)
        return topic
    
    def unsubscribe(self, topic):
        self.client.unsubscribe(topic)
        return topic
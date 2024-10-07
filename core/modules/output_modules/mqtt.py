import paho.mqtt.client as mqtt
import time
import logging
import json
from core.modules.output_modules.output_module import OutputModule
from core.metadata_manager.metadata import metadata_manager
FIRST_RECONNECT_DELAY = 1
RECONNECT_RATE = 2
MAX_RECONNECT_COUNT = 12
MAX_RECONNECT_DELAY = 60

log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
log_file = "paho_mqtt.log"

logging.basicConfig(level=logging.DEBUG, 
                    filename=log_file, 
                    filemode='w',
                    format=log_format)
logger = logging.getLogger()

class MQTT(OutputModule):
    def __init__(self, broker, port=None, 
                 username=None,password=None,fallback=None):
        super().__init__(fallback=fallback)
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_log = self.on_log
        self.client.on_message = self.on_message
        self.enable_logger()
        if port is None:
            port = 1883
        if username is not None and password is not None:
            self.client.username_pw_set(username,password)
        self.session_start_time = time.time()
        self.client.connect(broker, port, 60)
        self.client.loop_start()
        self.messages = {}

    
    def transmit(self, topic,data=None,retain=False):
        print(topic,data)
        if isinstance(data, dict):
            data = json.dumps(data)
        elif data is not None and not isinstance(data, str):
            data = str(data)
        result = self.client.publish(topic=topic, 
                                     payload=data, 
                                     qos=0, retain=retain)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            if self._fallback is not None:
                self._fallback.transmit(topic,data=data)
            else:
                logger.error(f"Failed to send message: {result.rc}")

    def get_existing_ids(self):
        topic = metadata_manager.details()
        self.subscribe(topic)
        time.sleep(2)
        self.unsubscribe(topic)
        ids = []
        for k,v in self.messages.items():
            if metadata_manager.is_called(k,topic):
                ids.append(metadata_manager.get_instance_id(k))
        self.reset_messages()
        return ids

    def flush(self,topic):
        self.client.publish(topic=topic, 
                            payload=None, 
                            qos=0, retain=True)
        
    
    def enable_logger(self):
        self.client.enable_logger()

    def disable_logger(self):
        self.client.disable_logger()

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
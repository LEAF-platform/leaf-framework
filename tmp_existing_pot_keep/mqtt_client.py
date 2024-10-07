# core/mqtt_client.py
import uuid

import paho.mqtt.client as mqtt
import logging

from paho.mqtt.enums import CallbackAPIVersion

from core.config_loader import Config

logging.basicConfig(level=logging.DEBUG)

class MQTTClient:
    def __init__(self, config: Config):
        # 2. Initialize the MQTT client
        # Initialize username and password attributes to None
        self.broker_host = config.get('mqtt', 'broker')
        self.broker_port = config.get_int('mqtt', 'port')
        ############################################################################################################
        # Credentials
        self.mqtt_username = None
        self.mqtt_password = None
        if config.has_option('mqtt', 'username'):
            self.mqtt_username = config.get('mqtt', 'username')
            logging.info(f"MQTT username: {self.mqtt_username}")
        if config.has_option('mqtt', 'password'):
            self.mqtt_password = config.get('mqtt', 'password')
            logging.info("MQTT password set")
        ############################################################################################################
        if config.has_option('mqtt', 'client_id'):
            self.mqtt_clientid = config.get('mqtt', 'clientid')
        else:
            self.mqtt_clientid = uuid.uuid4().hex
            logging.info(f"Generated client ID: {self.mqtt_clientid}")
        ############################################################################################################
        if config.has_option('mqtt', 'transport'):
            self.transport = config.get('mqtt', 'transport')
        else:
            self.transport = 'tcp'
        ############################################################################################################
        # Protocol
        if config.has_option('mqtt', 'protocol'):
            self.protocol = mqtt.MQTTv5 if '5' in config.get('mqtt', 'protocol')  else mqtt.MQTTv311
            logging.info(f"MQTT protocol: {self.protocol}")
        else:
            self.protocol = mqtt.MQTTv311
            logging.info(f"MQTT protocol: {self.protocol}")
        ############################################################################################################

        self.client: mqtt.Client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id=self.mqtt_clientid, protocol=self.protocol, transport=self.transport)

        # Attach callback methods for connection, messages, etc.
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

    def connect(self) -> None:
        """Connects to the MQTT broker."""
        logging.info(f"Connecting to MQTT broker at {self.broker_host}:{self.broker_port}")
        if self.mqtt_username and self.mqtt_password:
            self.client.username_pw_set(self.mqtt_username, self.mqtt_password)
        self.client.connect(self.broker_host, self.broker_port)
        self.client.loop_start()  # Start the network loop

    def on_connect(self, client: mqtt.Client, userdata: object, flags: object, reason_code, properties) -> None:
        """Callback when the client connects to the broker."""
        if reason_code == 0:
            logging.info("MQTT Client connected successfully.")
        else:
            logging.error(f"Failed to connect with result code {reason_code}")

    def subscribe(self, topic: str) -> None:
        """Subscribe to a given MQTT topic."""
        logging.info(f"Subscribing to topic: {topic}")
        self.client.subscribe(topic)

    def publish(self, topic: str, message: str) -> mqtt.MQTTMessageInfo:
        """Publish a message to a given MQTT topic and return the result."""
        return self.client.publish(topic, message)

    def on_message(self, client: mqtt.Client, userdata: object, msg: mqtt.MQTTMessage) -> None:
        """Callback when a message is received on a subscribed topic."""
        logging.info(f"Received message on {msg.topic}: {msg.payload.decode()}")

    def disconnect(self) -> None:
        """Disconnects from the MQTT broker."""
        logging.info("Disconnecting from MQTT broker")
        self.client.loop_stop()
        self.client.disconnect()

# Usage (from any adapter or the core):
# mqtt_client = MQTTClient(broker_host='your_broker_ip', broker_port=1883)
# mqtt_client.connect()
# mqtt_client.subscribe("your/topic")
# mqtt_client.publish("your/topic", "your message")


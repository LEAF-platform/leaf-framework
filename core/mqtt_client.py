# core/mqtt_client.py

import paho.mqtt.client as mqtt
import logging

logging.basicConfig(level=logging.DEBUG)

class MQTTClient:
    def __init__(self, broker_host: str, broker_port: int, broker_username: str, broker_password, client_id: str, protocol: mqtt.MQTTv5 = mqtt.MQTTv5, callback_api_version: int = 2) -> None:
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.broker_username = broker_username
        self.broker_password = broker_password
        self.client_id = client_id
        self.protocol = mqtt.MQTTv5
        self.callback_api_version = callback_api_version
        self.client: mqtt.Client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2
        )

        # Attach callback methods for connection, messages, etc.
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

    def connect(self) -> None:
        """Connects to the MQTT broker."""
        logging.info(f"Connecting to MQTT broker at {self.broker_host}:{self.broker_port}")
        if self.broker_username and self.broker_password:
            self.client.username_pw_set(self.broker_username, self.broker_password)
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

    def publish(self, topic: str, message: str) -> None:
        """Publish a message to a given MQTT topic."""
        logging.debug(f"Publishing message to topic {topic}: {message}")
        self.client.publish(topic, message)

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


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
    """
    Handles output via the MQTT protocol. Inherits from the abstract
    OutputModule class and is responsible for publishing data to an 
    MQTT broker. If transmission fails, it can use a fallback 
    OutputModule if one is provided.
    Adapter establishes a connection to the MQTT broker, manages 
    reconnections,and handles message publishing. It supports both 
    TCP and WebSocket transports, with optional TLS encryption 
    for secure communication.
    """
    
    def __init__(self, broker, port=1883, username=None, password=None, 
                 fallback=None, clientid: Optional[str] = None, protocol="v3", 
                 transport: Literal['tcp', 'websockets', 'unix'] = 'tcp', 
                 tls: bool = False):
        """
        Initialise the MQTT adapter with broker details, 
        authentication, and optional fallback.

        Args:
            broker: The address of the MQTT broker.
            port: The port number (default is 1883).
            username: Optional username for authentication.
            password: Optional password for authentication.
            fallback: Another OutputModule to use if the MQTT transmission fails.
            clientid: Optional client ID to use for the MQTT connection.
            protocol: MQTT protocol version ("v3" for MQTTv3.1.1, "v5" for MQTTv5).
            transport: The transport method, either TCP, WebSockets, or Unix socket.
            tls: Boolean flag to enable or disable TLS encryption.
        """
        super().__init__(fallback=fallback)
        
        self.protocol = mqtt.MQTTv5 if '5' in protocol.__str__() else mqtt.MQTTv311
        logger.debug(f"MQTT protocol: {self.protocol}")
        logger.debug(f"MQTT transport: {transport}")
        logger.debug(f"MQTT client ID: {clientid}")
        logger.debug(f"MQTT TLS: {tls}")

        self.client = mqtt.Client(client_id=clientid, 
                                  protocol=self.protocol, 
                                  transport=transport)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_connect_failure = self.on_connect_failure
        self.client.on_log = self.on_log
        self.client.on_message = self.on_message
        self.session_start_time = time.time()

        if username is not None and password is not None:
            logger.debug(f"MQTT username: {username}")
            self.client.username_pw_set(username, password)
        
        if tls:
            logger.debug(f"MQTT TLS enabled")
            self.client.tls_set()
            self.client.tls_insecure_set(True)

        logger.debug(f"Connecting to MQTT broker at {broker}:{port}")
        self.client.connect(broker, port, 60)
        self.client.loop_start()
        self.messages = {}

    
    def transmit(self, topic, data=None, retain=False):
        """
        Publish a message to the MQTT broker on a given topic.

        Args:
            topic: The topic to publish the message to.
            data: The message payload to be transmitted.
            retain: Whether to retain the message on the broker.
        """
        if isinstance(data, (dict, list)):
            data = json.dumps(data)
        elif data is not None and not isinstance(data, str):
            data = str(data)
        elif data is None:
            logger.error(f"No data was provided")
            data = ""
        
        logger.debug(f"Transmitting message to {topic}: {data[:50]}")
        result = self.client.publish(topic=topic, payload=data, qos=0, 
                                     retain=retain)

        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            if self._fallback is not None:
                self._fallback.transmit(topic, data=data)
            else:
                logger.error(f"Failed to send message: {result.rc}")

    def flush(self, topic):
        """
        Clear any retained messages on the broker 
        by publishing an empty payload.

        Args:
            topic: The topic to clear retained messages for.
        """
        self.client.publish(topic=topic, payload=None, qos=0, retain=True)
        
    def on_connect(self, client, userdata, flags, rc, metadata=None):
        """
        Callback for when the client connects to the broker.

        Args:
            client: The MQTT client instance.
            userdata: The private user data as set in 
                      Client() or userdata_set().
            flags: Response flags sent by the broker.
            rc: The connection result code.
            metadata: Additional metadata (if any).
        """
        if rc != 0:
            logger.error(f"Failed to connect: {rc}")

    def on_disconnect(self,client, userdata, rc):
        """
        Callback for when the client disconnects from the broker.

        Args:
            client: The MQTT client instance.
            userdata: The private user data as set in 
                      Client() or userdata_set().
            flags: Response flags sent by the broker.
            rc: The disconnection result code.
            metadata: Additional metadata (if any).
        """
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
        
        logger.error(f"Unable to reconnect after {MAX_RECONNECT_COUNT} attempts")

    def on_connect_failure(self, client, userdata, flags, rc, metadata):
        """
        Callback for when the connection attempt to the broker fails.

        Args:
            client: The MQTT client instance.
            userdata: The private user data as set in Client() 
                      or userdata_set().
            flags: Response flags sent by the broker.
            rc: The connection failure result code.
            metadata: Additional metadata (if any).
        """
        logger.error(f"Failed to connect: {rc}")

    def on_log(self, client, userdata, paho_log_level, message):
        """
        Callback for logging MQTT client activity.

        Args:
            client: The MQTT client instance.
            userdata: The private user data as set in 
                      Client() or userdata_set().
            paho_log_level: The log level set for the client.
            message: The log message.
        """
        if paho_log_level == mqtt.LogLevel.MQTT_LOG_ERR:
            print(message, paho_log_level)

    def on_message(self, client, userdata, msg):
        """
        Callback for when a message is received on a 
        subscribed topic.

        Args:
            client: The MQTT client instance.
            userdata: The private user data as 
                      set in Client() or userdata_set().
            msg: The received MQTT message.
        """
        payload = msg.payload.decode()
        topic = msg.topic
        if topic not in self.messages:
            self.messages[topic] = []
        self.messages[topic].append(payload)

    def reset_messages(self):
        """Clear all stored messages."""
        self.messages = {}

    def subscribe(self, topic):
        """
        Subscribe to a topic on the MQTT broker.

        Args:
            topic: The topic to subscribe to.

        Returns:
            The subscribed topic.
        """
        self.client.subscribe(topic)
        return topic
    
    def unsubscribe(self, topic):
        """
        Unsubscribe from a topic on the MQTT broker.

        Args:
            topic: The topic to unsubscribe from.

        Returns:
            The unsubscribed topic.
        """
        self.client.unsubscribe(topic)
        return topic

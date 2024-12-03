import json
import time
from socket import error as socket_error
from socket import gaierror
from typing import Literal, Union, Any

from leaf import start
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

from leaf.error_handler.error_holder import ErrorHolder
from leaf.error_handler.exceptions import AdapterBuildError, LEAFError
from leaf.error_handler.exceptions import ClientUnreachableError
from leaf.error_handler.exceptions import SeverityLevel
from leaf.modules.logger_modules.logger_utils import get_logger
from leaf.modules.output_modules.output_module import OutputModule

FIRST_RECONNECT_DELAY = 1
RECONNECT_RATE = 2
MAX_RECONNECT_COUNT = 12
MAX_RECONNECT_DELAY = 1

logger = get_logger(__name__, log_file="app.log", log_level=logging.ERROR)

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

    def __init__(self, broker: str, port: int = 1883, 
                 username: Union[str, None] = None, 
                 password: Union[str, None] = None, fallback=None, 
                 clientid: Union[str, None] = None, protocol: str = "v3",
                 transport: Literal['tcp', 'websockets', 'unix'] = 'tcp',
                 tls: bool = False, error_holder: ErrorHolder = None) -> None:
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

        super().__init__(fallback=fallback, error_holder=error_holder)

        if protocol not in ["v3", "v5"]:
            raise AdapterBuildError(f"Unsupported protocol '{protocol}'.")
        self.protocol = mqtt.MQTTv5 if protocol == "v5" else mqtt.MQTTv311

        if transport not in ["tcp", "websockets", "unix"]:
            raise AdapterBuildError(f"Unsupported transport '{transport}'.")

        if not isinstance(broker, str) or not broker:
            raise AdapterBuildError("Broker must be a non-empty string representing the MQTT broker address.")
        if not isinstance(port, int) or not (1 <= port <= 65535):
            raise AdapterBuildError("Port must be an integer between 1 and 65535.")

        self._client_id = clientid
        self._broker = broker
        self._port = port
        self._username = username
        self._password = password
        self._tls = tls
        self.messages = {}

        self.client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2,
                                  client_id=clientid,
                                  protocol=self.protocol, 
                                  transport=transport)
        self.client.on_connect = self.on_connect
        self.client.on_connect_fail = self.on_connect_fail
        self.client.on_disconnect = self.on_disconnect
        self.client.on_log = self.on_log
        self.client.on_message = self.on_message

        if username and password:
            self.client.username_pw_set(username, password)
        if tls:
            try:
                self.client.tls_set()
                self.client.tls_insecure_set(True)
            except Exception as e:
                raise AdapterBuildError(f"Failed to set up TLS: {e}")
            
        self.connect()

    def connect(self) -> None:
        """
        Connects to the MQTT broker and sets a thread looping. 
        """
        if not self._enabled:
            logger.warning(f'{self.__class__.__name__} - connect called with module disabled.')
            return
        try:
            self.client.connect(self._broker, self._port, 60)
            time.sleep(0.5)
            self.client.loop_start()
        except (socket_error, gaierror, OSError) as e:
            self._handle_exception(ClientUnreachableError(f"Error connecting to broker: {e}", 
                                                          output_module=self))

    def disconnect(self) -> None:
        """
        Disconnected from the MQTT broker and stops the threaded loop.
        """
        try:
            if self.client.is_connected():
                self.client.disconnect()
                time.sleep(0.5)
            self.client.loop_stop()
            logger.info("Disconnected from MQTT broker.")
        except Exception as e:
            logger.error(f"Failed to disconnect from MQTT broker: {e}")
            self._handle_exception(ClientUnreachableError("Failed to disconnect from broker.", 
                                                          output_module=self))
            
    def transmit(self, topic: str, data: Union[str, dict, None] = None,
                 retain: bool = False):
        """
        Publish a message to the MQTT broker on a given topic.

        Args:
            topic: The topic to publish the message to.
            data: The message payload to be transmitted.
            retain: Whether to retain the message on the broker.
        """
        if not self._enabled:
            logger.warning(f'{self.__class__.__name__} - transmit called with module disabled.')
            return False

        if not self.client.is_connected():
            return self.fallback(topic, data)

        if isinstance(data, (dict,list)):
            data = json.dumps(data)
        elif data is None:
            data = ""

        try:
            result = self.client.publish(topic=topic, payload=data, 
                                        qos=0, retain=retain)
        except ValueError:
            msg = f'{topic} contains wildcards, likely required instance data missing'
            exception = ClientUnreachableError(msg, output_module=self, 
                                               severity=SeverityLevel.ERROR)
            self._handle_exception(exception)
            return False
        
        error = self._handle_return_code(result.rc)
        if error is not None:
            self._handle_exception(error)
            return self.fallback(topic, data)
        return True

    def flush(self, topic: str) -> None:
        """
        Clear any retained messages on the broker 
        by publishing an empty payload.

        Args:
            topic: The topic to clear retained messages for.
        """
        if not self._enabled:
            logger.warning(f'{self.__class__.__name__} - flush called with module disabled.')
            return
        try:
            result = self.client.publish(topic=topic, payload=None, 
                                         qos=0, retain=True)
            error = self._handle_return_code(result.rc)
            if error is not None:
                self._handle_exception(error)
                return self.fallback(topic, None)
        except ValueError:
            msg = f'{topic} contains wildcards, likely required instance data missing'
            exception = ClientUnreachableError(msg, output_module=self, 
                                               severity=SeverityLevel.CRITICAL)
            self._handle_exception(exception)

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
        logger.debug(f"Connected: {rc}")
        if rc != 0:
            error_messages = {
                1: 'Unacceptable protocol version',
                2: 'Client identifier rejected',
                3: 'Server unavailable',
                4: 'Bad username or password',
                5: 'Not authorized'
            }
            message = error_messages.get(rc, f"Unknown connection error with code {rc}")
            self._handle_exception(ClientUnreachableError(f"Connection refused: {message}", output_module=self))

    def on_connect_fail(self, client, userdata, flags, rc, metadata=None):
        """
        Callback for when the client fails to connect to the broker.

        Args:
            client: The MQTT client instance.
            userdata: The private user data as set in
                      Client() or userdata_set().
            flags: Response flags sent by the broker.
            rc: The connection result code.
            metadata: Additional metadata (if any).
        """
        logger.error(f"Connection failed: {rc}")
        leaf_error = LEAFError("Failed to connect", SeverityLevel.CRITICAL)
        self._handle_exception(leaf_error)

    def on_disconnect(self, client: mqtt.Client, userdata: Any, flags: mqtt.DisconnectFlags, rc, properties= None) -> None:
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
        if rc != mqtt.MQTT_ERR_SUCCESS:
            reconnect_count, reconnect_delay = 0, FIRST_RECONNECT_DELAY
            while reconnect_count < MAX_RECONNECT_COUNT:
                time.sleep(reconnect_delay)
                try:
                    client.reconnect()
                    return
                except Exception:
                    reconnect_delay = min(reconnect_delay * RECONNECT_RATE, MAX_RECONNECT_DELAY)
                    reconnect_count += 1
            self._handle_exception(ClientUnreachableError("Failed to reconnect.", output_module=self))

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
        logger.debug(f'{paho_log_level} : {message}')

    def on_message(self, client: mqtt.Client, userdata: str, 
                   msg: mqtt.MQTTMessage) -> None:
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

    def reset_messages(self) -> None:
        self.messages = {}

    def subscribe(self, topic: str) -> str:
        """
        Subscribe to a topic on the MQTT broker.

        Args:
            topic: The topic to subscribe to.

        Returns:
            The subscribed topic.
        """
        logger.debug(f"Subscribing to {topic}")
        self.client.subscribe(topic)
        return topic
    
    def unsubscribe(self, topic: str) -> str:
        """
        Unsubscribe from a topic on the MQTT broker.

        Args:
            topic: The topic to unsubscribe from.

        Returns:
            The unsubscribed topic.
        """

        self.client.unsubscribe(topic)
        return topic

    def enable(self):
        '''
        Reenables an output transmitting.
        Only needs to be called if the disable 
        function has been called previously.
        '''
        if self.client.is_connected():
            self.disconnect()
        return super().enable()

    def disable(self):
        '''
        Stops an output from transmitting.
        This will be used to disable output modules which arent 
        working for whatever reason to stop them locking the system.
        '''
        if not self.client.is_connected():
            self.connect()
        return super().disable()
    

    def _handle_return_code(self, return_code):
        if return_code == mqtt.MQTT_ERR_SUCCESS:
            return None
        message = {
            mqtt.MQTT_ERR_NO_CONN: "Can't connect to broker",
            mqtt.MQTT_ERR_QUEUE_SIZE: "Message queue size limit reached"
        }.get(return_code, f"Unknown error with return code {return_code}")

        return ClientUnreachableError(message, output_module=self, 
                                      severity=SeverityLevel.INFO)
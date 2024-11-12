import logging
import redis
from typing import Optional, Any
from core.modules.output_modules.output_module import OutputModule
from core.error_handler.exceptions import ClientUnreachableError
from core.error_handler.exceptions import SeverityLevel
from core.error_handler.error_holder import ErrorHolder


class KEYDB(OutputModule):
    """
    An output module for interacting with a KeyDB (Redis-compatible) 
    server. This class provides methods to connect to KeyDB, transmit 
    data, retrieve data, and handle errors consistently. If connection 
    or transmission fails, a fallback module can be used if provided.
    """

    def __init__(self, host: str, port: int = 6379, db: int = 0, 
                 fallback: Optional[OutputModule] = None, 
                 error_holder: Optional[ErrorHolder] = None) -> None:
        """
        Initialize the KEYDB adapter with KeyDB connection details and 
        optional fallback.

        Args:
            host (str): The KeyDB server hostname or IP address.
            port (int): The port for KeyDB connection (default is 6379).
            db (int): The database number to connect to (default is 0).
            fallback (Optional[OutputModule]): Fallback module to use 
                     if KeyDB operations fail.
            error_holder (Optional[Any]): Optional error holder 
                         for tracking errors.
        """
        super().__init__(fallback=fallback, error_holder=error_holder)
        self.host = host
        self.port = port
        self.db = db
        self._client: Optional[redis.StrictRedis] = None

    def _handle_redis_error(self, exception: redis.RedisError) -> None:
        """
        Handle Redis-related errors by logging 
        them and invoking the error handler.

        Args:
            exception (redis.RedisError): The Redis exception that occurred.
        """
        if isinstance(exception, redis.AuthenticationError):
            message = f"Authentication failed for KeyDB at {self.host}:{self.port}"
            severity = SeverityLevel.CRITICAL
        elif isinstance(exception, redis.ConnectionError):
            if "Network is unreachable" in str(exception):
                message = f"Network unreachable for KeyDB at {self.host}:{self.port}"
                severity = SeverityLevel.ERROR
            elif "Connection refused" in str(exception):
                message = f"Connection refused for KeyDB at {self.host}:{self.port}"
                severity = SeverityLevel.WARNING
            else:
                message = f"Failed to connect to KeyDB at {self.host}:{self.port}: {str(exception)}"
                severity = SeverityLevel.CRITICAL
        elif isinstance(exception, redis.TimeoutError):
            message = f"Connection to KeyDB at {self.host}:{self.port} timed out."
            severity = SeverityLevel.ERROR
        else:
            message = f"Redis error for KeyDB: {str(exception)}"
            severity = SeverityLevel.WARNING

        self._handle_exception(ClientUnreachableError(message, 
                                                      output_module=self, 
                                                      severity=severity))

    def connect(self) -> None:
        """
        Establish a connection to the KeyDB server. 

        Logs success or handles connection errors by 
        invoking the error handler.
        """
        try:
            self._client = redis.StrictRedis(host=self.host, port=self.port, db=self.db)
            logging.info("Connected to KeyDB.")
        except redis.RedisError as e:
            self._handle_redis_error(e)

    def transmit(self, topic: str, data: Optional[Any] = None) -> bool:
        """
        Transmit data to the KeyDB server by setting 
        the value for a given key.

        Args:
            topic (str): The key name under which the 
                         data will be stored.
            data (Optional[Any]): The data to store in KeyDB.

        Returns:
            bool: True if the data was successfully transmitted, 
                  False if a fallback was used.
        """
        if self._client is None:
            return self.fallback(topic, data)
        try:
            self._client.set(topic, data)
            logging.info(f"Transmit data to key '{topic}' in KeyDB.")
            return True
        except redis.RedisError as e:
            self._handle_redis_error(e)
            return self.fallback(topic, data)

    def disconnect(self) -> None:
        """
        Disconnect from the KeyDB server by 
        setting the client to None. 

        Logs the disconnection status.
        """
        if self._client is not None:
            self._client = None
            logging.info("Disconnected from KeyDB.")
        else:
            logging.info("Already disconnected from KeyDB.")

    def retrieve(self, key: str) -> Optional[str]:
        """
        Retrieve data from KeyDB for a given key.

        Args:
            key (str): The key name for which to retrieve data.

        Returns:
            Optional[str]: The retrieved data as a UTF-8 decoded 
                           string, or None if not found.
        """
        if self._client is None:
            return None
        try:
            message = self._client.get(key)
            return message.decode('utf-8') if message else None
        except redis.RedisError as e:
            self._handle_redis_error(e)
            return None

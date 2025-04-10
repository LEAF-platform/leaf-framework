import json

from typing import Optional, Any

import redis

from leaf.error_handler.error_holder import ErrorHolder
from leaf.error_handler.exceptions import ClientUnreachableError
from leaf.error_handler.exceptions import SeverityLevel
from leaf.modules.output_modules.output_module import OutputModule
from leaf.start import logger


class KEYDB(OutputModule):
    """
    An output module for interacting with a KeyDB (Redis-compatible)
    server. This class provides methods to connect to KeyDB, transmit
    data, retrieve data, and handle errors consistently. If connection
    or transmission fails, a fallback module can be used if provided.
    """

    def __init__(
        self,
        host: str,
        port: int = 6379,
        db: int = 0,
        fallback: Optional[OutputModule] = None,
        error_holder: Optional[ErrorHolder] = None,
    ) -> None:
        """
        Initialize the KEYDB adapter with KeyDB connection details and
        optional fallback.

        Args:
            host (str): The KeyDB server hostname or IP address.
            port (int): The port for KeyDB connection (default is 6379).
            db (int): The database number to connect to (default is 0).
            fallback (Optional[OutputModule]): Fallback module to use
                     if KeyDB operations fail.
            error_holder (Optional[ErrorHolder]): Optional error holder
                         for tracking errors.
        """
        super().__init__(fallback=fallback, error_holder=error_holder)
        self.host: str = host
        self.port: int = port
        self.db: int = db
        self._client: Optional[redis.StrictRedis] = None
        self.connect()

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

        self._handle_exception(
            ClientUnreachableError(message, output_module=self, severity=severity)
        )

    def connect(self) -> None:
        """
        Establish a connection to the KeyDB server.

        Logs success or handles connection errors by
        invoking the error handler.
        """
        try:
            self._client = redis.StrictRedis(host=self.host, port=self.port, db=self.db)
            logger.info("Connected to KeyDB.")
        except redis.RedisError as e:
            self._handle_redis_error(e)

    def transmit(self, topic: str, data: Optional[Any] = None) -> bool:
        """
        Transmit data to the KeyDB server by appending it to the existing
        value for the given key. The value for each key is always stored
        as a list, even if it contains only one element.

        Args:
            topic (str): The key name under which the data will be stored.
            data (Optional[Any]): The data to append to the list in KeyDB.

        Returns:
            bool: True if the data was successfully transmitted,
                False if a fallback was used.
        """
        if data is None:
            logger.warning(f"No data provided to transmit ({topic}).")
            return False

        if isinstance(data, dict):
            data = json.dumps(data)

        if self._client is None:
            return self.fallback(topic, data)

        try:
            current_value = self._client.get(topic)
            if current_value:
                current_list = json.loads(current_value.decode("utf-8"))
                if not isinstance(current_list, list):
                    logger.error(
                        f"Unexpected value format for key '{topic}'. Overwriting."
                    )
                    current_list = []
                current_list.append(data)
            else:
                current_list = [data]

            self._client.set(topic, json.dumps(current_list))
            logger.info(f"Appended data to key '{topic}' in KeyDB.")
            return True
        except redis.RedisError as e:
            self._handle_redis_error(e)
            return self.fallback(topic, data)

    def is_connected(self) -> bool:
        """
        Check if the connection to KeyDB is established.

        Returns:
            bool: True if connected, False otherwise.
        """
        return self._client is not None

    def disconnect(self) -> None:
        """
        Disconnect from the KeyDB server by
        setting the client to None.

        Logs the disconnection status.
        """
        if self._client is not None:
            self._client = None
            logger.info("Disconnected from KeyDB.")
        else:
            logger.info("Already disconnected from KeyDB.")

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
            return message.decode("utf-8") if message else None
        except redis.RedisError as e:
            self._handle_redis_error(e)
            return None

    def pop(self, key: Optional[str] = None) -> Optional[tuple[str, Any]]:
        """
        Retrieve and remove a record from KeyDB.
        If a specific key is provided, retrieve and remove all values under
        that key. If no key is provided, retrieve and remove one element from
        a random key's list. The key is removed when the last element is taken.

        Args:
            key (Optional[str]): The key of the record to retrieve and remove.
                                If None, a random record is retrieved and removed.

        Returns:
            Optional[tuple[str, Any]]: A tuple of the key and the retrieved value,
                                    or None if the key does not exist or the
                                    database is empty.
        """
        if self._client is None:
            logger.warning("Attempted to pop from a disconnected KeyDB client.")
            return None

        try:
            if key is not None:
                result = self._client.get(key)
                if result:
                    self._client.delete(key)
                    return key, json.loads(result.decode("utf-8"))
                return None

            random_key = self._client.randomkey()
            if not random_key:
                logger.info("No keys available in KeyDB.")
                return None

            random_key = random_key.decode("utf-8")
            result = self._client.get(random_key)

            if not result:
                return None

            result = json.loads(result.decode("utf-8"))
            if isinstance(result, list):
                popped_value = result.pop(0)
                if result:
                    self._client.set(random_key, json.dumps(result))
                else:
                    self._client.delete(random_key)
                return random_key, popped_value

            logger.error(
                f"Unexpected value type for key '{random_key}'. Deleting key."
            )
            self._client.delete(random_key)
            return None
        except redis.RedisError as e:
            self._handle_redis_error(e)
            return None

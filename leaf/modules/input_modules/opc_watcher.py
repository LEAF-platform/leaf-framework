import time
from typing import Optional, Callable, List, Any, Set

from leaf_register.metadata import MetadataManager

try:
    from opcua import Client, Node, Subscription  # type: ignore
    from opcua.ua import DataChangeNotification   # type: ignore
    OPCUA_AVAILABLE = True
    # Suppress OPC UA library logging to avoid cluttering the output
    import logging
    logging.getLogger('opcua').setLevel(logging.WARNING)
except ImportError:
    from leaf.start import logger
    logger.warning("OPC UA library not available. OPCWatcher will not function.")
    OPCUA_AVAILABLE = False
    Client = Node = Subscription = DataChangeNotification = None  # Placeholders

from leaf.error_handler.error_holder import ErrorHolder
from leaf.modules.input_modules.event_watcher import EventWatcher

class OPCWatcher(EventWatcher):
    """
    A concrete implementation of EventWatcher that uses
    predefined fetchers to retrieve and monitor data.
    """

    def __init__(self,
                 metadata_manager: MetadataManager,
                 host: str,
                 port: int,
                 topics: set[str],
                 exclude_topics: list[str],
                 callbacks: Optional[List[Callable[..., Any]]] = None,
                 error_holder: Optional[ErrorHolder] = None) -> None:
        """
        Initialize OPCWatcher.

        Args:
            metadata_manager (MetadataManager): Manages equipment metadata.
            callbacks (Optional[List[Callable]]): Callbacks for event updates.
            error_holder (Optional[ErrorHolder]): Optional object to manage errors.
        """
        super().__init__(metadata_manager,
                         callbacks=callbacks,
                         error_holder=error_holder)

        self._host = host
        self._port = port
        self._topics: set[str] = topics
        self._exclude_topics: list[str] = exclude_topics
        self._metadata_manager = metadata_manager
        self._sub: Subscription|None = None
        self._handler = self._dispatch_callback
        self._handles: list[Any] = []

    def datachange_notification(self, node: Node, val: int|str|float, data: DataChangeNotification) -> None:
        self._dispatch_callback(self._metadata_manager.experiment.measurement, {
            "node": node.nodeid.Identifier,
            "value":val,
            "timestamp":data.monitored_item.Value.SourceTimestamp,
            "data":data
        })

    def start(self) -> None:
        """
        Start the OPCWatcher
        """
        if not OPCUA_AVAILABLE:
            raise Exception("OPC UA library is not available. Cannot start OPCWatcher.")

        print(f"Starting OPCWatcher on {self._host}:{self._port}")
        self._client = Client(f"opc.tcp://{self._host}:{self._port}")
        self._client.connect()

        root = self._client.get_root_node()
        objects_node = root.get_child(["0:Objects"])
        # Automatically browse and read nodes to obtain topics user could provide a list of topics.
        if self._topics is None or len(self._topics) == 0:
            print("No topics provided. Browsing and reading all nodes.")
            self._topics = self._browse_and_read(objects_node)
            for topic in self._topics:
                print(f"Found topic: {topic}")

        print(f"Number of topics: {len(self._topics)}")

        # Subscribe to topics
        self._subscribe_to_topics()

    def _browse_and_read(self, node: Node) -> Set[str]:
        """
        Recursively browse and read OPC UA nodes to obtain topics.

        Returns:
            Set[str]: A set of node identifiers (NodeIds).
        """
        nodes_data = set()
        for child in node.get_children():
            browse_name = child.get_browse_name().Name
            if browse_name == "Server":
                continue
            try:
                child.get_value()
                nodes_data.add(child.nodeid.Identifier)
            except Exception as e:
                from leaf.start import logger
                logger.error(e)
                pass
            nodes_data.update(self._browse_and_read(child))  # Recursive call
        return nodes_data

    def _subscribe_to_topics(self) -> None:
        """
        Subscribe to OPC UA nodes and monitor data changes.
        """
        from leaf.start import logger

        if not self._client:
            print("Client is not connected.")
            return
        try:
            self._sub = self._client.create_subscription(1000, self)  # 1s interval
            for topic in self._topics:
                if topic in self._exclude_topics:
                    logger.info("Excluded topic: {}".format(topic))
                    continue
                try:
                    node = self._client.get_node(f"ns=2;s={topic}")  # Adjust namespace
                    handle = self._sub.subscribe_data_change(node)
                    self._handles.append(handle)
                    print(f"Subscribed to: {topic}")
                except Exception as e:
                    print(f"Failed to subscribe to {topic}: {e}")
                    if "ServiceFault" in str(e):
                        print("Retrying in 5 seconds...")
                        time.sleep(5)
                        continue  # Try the next topic
        except Exception as e:
            print(f"Failed to create subscription: {e}")
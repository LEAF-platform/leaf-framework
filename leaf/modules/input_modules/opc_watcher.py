from typing import Optional, Callable, List, Any

from leaf_register.metadata import MetadataManager

try:
    from opcua import Client, Node, Subscription
    from opcua.ua import DataChangeNotification
    OPCUA_AVAILABLE = True
except ImportError:
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
                 topics: Optional[List[str]] = [],
                 exclude_topics: Optional[List[str]] = [],
                 callbacks: Optional[List[Callable]] = None,
                 error_holder: Optional[ErrorHolder] = None) -> None:
        """
        Initialize OPCWatcher.

        Args:
            metadata_manager (MetadataManager): Manages equipment metadata.
            callbacks (Optional[List[Callable]]): Callbacks for event updates.
            error_holder (Optional[ErrorHolder]): Optional object to manage errors.
        """
        if not OPCUA_AVAILABLE:
            raise ImportError("opcua module is required for OPCWatcher functionality.")

        # Can't populate yet in this situation
        term_map: dict[Any, Any] = {}

        super().__init__(term_map, metadata_manager,
                         callbacks=callbacks,
                         error_holder=error_holder)

        self._host = host
        self._port = port
        self._topics: list[str] | None = topics
        self._exclude_topics: list[str] | None = exclude_topics
        self._metadata_manager = metadata_manager
        self._client: Client | None = None
        self._sub: Subscription | None = None
        self._handler = self._dispatch_callback
        self._handles: list[Any] = []

        # Ensure import-safe behavior before assigning OPCUA-related functions
        if OPCUA_AVAILABLE:
            self._term_map[self.datachange_notification] = metadata_manager.experiment.measurement

    def datachange_notification(self, node: Node, val: int | str | float, data: DataChangeNotification) -> None:
        if not OPCUA_AVAILABLE:
            return
        self._dispatch_callback(self.datachange_notification, {
            "node": node.nodeid.Identifier,
            "value": val,
            "timestamp": data.monitored_item.Value.SourceTimestamp,
            "data": data
        })

    def start(self) -> None:
        """
        Start the OPCWatcher
        """
        if not OPCUA_AVAILABLE:
            raise RuntimeError("opcua module is not installed, cannot start OPCWatcher.")

        print(f"Starting OPCWatcher on {self._host}:{self._port}")
        self._client = Client(f"opc.tcp://{self._host}:{self._port}")
        self._client.connect()
        root = self._client.get_root_node()
        objects_node = root.get_child(["0:Objects"])

        if self._topics is None or len(self._topics) == 0:
            print("No topics provided. Browsing and reading all nodes.")
            self._topics = self._browse_and_read(objects_node)

        self._subscribe_to_topics()

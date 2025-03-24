from typing import Optional, Callable, List, Dict, Set
from leaf_register.metadata import MetadataManager
from opcua import Client, ua
from leaf.modules.input_modules.polling_watcher import PollingWatcher
from leaf.error_handler.error_holder import ErrorHolder


class SubHandler:
    """Handles OPC UA data change notifications."""

    def datachange_notification(self, node, val, data) -> None:
        print(f"Value changed: {node.nodeid}: {val}")


class OPCWatcher(PollingWatcher):
    """
    A concrete implementation of PollingWatcher that uses
    predefined fetchers to retrieve and monitor data.
    """

    def __init__(self,
                 metadata_manager: MetadataManager,
                 interval: int,
                 callbacks: Optional[List[Callable]] = None,
                 error_holder: Optional[ErrorHolder] = None) -> None:
        """
        Initialize OPCWatcher.

        Args:
            metadata_manager (MetadataManager): Manages equipment metadata.
            interval (int): Polling interval in seconds.
            callbacks (Optional[List[Callable]]): Callbacks for event updates.
            error_holder (Optional[ErrorHolder]): Optional object to manage errors.
        """
        super().__init__(interval, metadata_manager,
                         callbacks=callbacks,
                         error_holder=error_holder)

        self._interval = interval
        self._metadata_manager = metadata_manager
        self.client = None
        self.sub = None
        self.handler = SubHandler()

    def tester(self) -> None:
        """
        Test the OPCWatcher class by connecting and subscribing to topics.
        """
        self.host = '10.22.196.201'
        self.port = 49580
        print("Initializing MBP OPC Interpreter")

        self.client = Client(f"opc.tcp://{self.host}:{self.port}")
        self.client.connect()
        print("Connected to MBP OPC Server")

        # Browse for available nodes
        root = self.client.get_root_node()
        objects_node = root.get_child(["0:Objects"])
        self.topics = self.browse_and_read(objects_node)

        # Subscribe to topics
        self.subscribe_to_topics()

    def browse_and_read(self, node) -> Set[str]:
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
            except Exception:
                pass
            if len(nodes_data) > 10: return nodes_data
            nodes_data.update(self.browse_and_read(child))  # Recursive call
        return nodes_data

    def subscribe_to_topics(self) -> None:
        """
        Subscribe to OPC UA nodes and monitor data changes.
        """
        if not self.client:
            print("Client is not connected.")
            return

        self.sub = self.client.create_subscription(1000, self.handler)  # 1s interval
        for topic in self.topics:
            try:
                node = self.client.get_node(f"ns=2;s={topic}")  # Adjust namespace
                self.sub.subscribe_data_change(node)
                print(f"Subscribed to: {topic}")
            except Exception as e:
                print(f"Failed to subscribe to {topic}: {e}")

    def _fetch_data(self) -> Dict[str, Dict[str, str]]:
        """
        Fetch dummy data for testing and triggering callbacks.

        Returns:
            Dict[str, Dict[str, str]]: Example data to
                                       simulate event triggers.
        """
        return {"measurement": {"data": "data"}}

if __name__ == "__main__":
    watcher = OPCWatcher(MetadataManager(), 10)
    watcher.tester()

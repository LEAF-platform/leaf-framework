from typing import Optional, Callable, List, Set

from leaf_register.metadata import MetadataManager
from opcua import Client

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
                 topics: Optional[List[str]] = None,
                 callbacks: Optional[List[Callable]] = None,
                 error_holder: Optional[ErrorHolder] = None) -> None:
        """
        Initialize OPCWatcher.

        Args:
            metadata_manager (MetadataManager): Manages equipment metadata.
            callbacks (Optional[List[Callable]]): Callbacks for event updates.
            error_holder (Optional[ErrorHolder]): Optional object to manage errors.
        """
        # Can't populate yet in this situation
        term_map = {}
        super().__init__(term_map, metadata_manager,
                         callbacks=callbacks,
                         error_holder=error_holder)

        self._host = host
        self._port = port
        self._topics = topics

        self._metadata_manager = metadata_manager
        self._client = None
        self._sub = None
        self._handler = self._dispatch_callback
        self._handles = []

        # This is under the impression that the watcher will only ever express measurements.
        # Not control information such as when experiments start.
        self._term_map[self.datachange_notification] = metadata_manager.experiment.measurement

        '''
        If you do have other actions, then youd add more handlers i think, like below.
        But your OPC subscription system would need to subscribe to the correct topics using the appropriate handlers.
        self._start_handler = SubHandler(self._dispatch_callback)
        self._term_map[self._start_handler.datachange_notification] = metadata_manager.experiment.start'
        '''

    def datachange_notification(self, node, val, data) -> None:
        print(f"Value changed: {node.nodeid}: {val}")
        # I dont know what node, val and data pertain to.
        # If they are gonna always be different types of measurements then youd probably want to create a merged dict.
        # If it can describe actions, experiment start, stop, measurement etc then more complex behaviour is needed.
        # Perhaps a SubHandler for each type of action in the term_map
        self._dispatch_callback(self.datachange_notification, data)

    def start(self) -> None:
        """
        Start the OPCWatcher
        """
        print(f"Starting OPCWatcher on {self._host}:{self._port}")
        self._client = Client(f"opc.tcp://{self._host}:{self._port}")
        self._client.connect()

        # Not sure whats going on here. Could be changed to allow user defined topics etc.
        # Are they all different measurements?
        root = self._client.get_root_node()
        objects_node = root.get_child(["0:Objects"])
        # Automatically browse and read nodes to obtain topics user could provide a list of topics.
        self._topics = self._browse_and_read(objects_node)
        self._subscribe_to_topics()

        try:
            while self._running:
                time.sleep(1)
        finally:
            for handle in self._handles:
                self._sub.unsubscribe(handle)
            self._sub.delete()
            self._client.disconnect()

    def _browse_and_read(self, node) -> Set[str]:
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
            nodes_data.update(self._browse_and_read(child))  # Recursive call
        return nodes_data

    def _subscribe_to_topics(self) -> None:
        """
        Subscribe to OPC UA nodes and monitor data changes.
        """
        if not self._client:
            print("Client is not connected.")
            return

        self._sub = self._client.create_subscription(1000, self)  # 1s interval
        for topic in self._topics:
            try:
                node = self._client.get_node(f"ns=2;s={topic}")  # Adjust namespace
                handle = self._sub.subscribe_data_change(node)
                self._handles.append(handle)
                print(f"Subscribed to: {topic}")
            except Exception as e:
                print(f"Failed to subscribe to {topic}: {e}")


if __name__ == "__main__":
    host = '10.22.196.201'
    port = 49580
    watcher = OPCWatcher(MetadataManager(), host=host, port=port)
    watcher.start()
    # watcher.tester()

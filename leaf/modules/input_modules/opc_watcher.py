import time
from typing import Optional, Callable, List, Set

from leaf_register.metadata import MetadataManager
from opcua import Client, Node
from opcua.ua import DataChangeNotification

import leaf.start
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
        # Can't populate yet in this situation
        term_map = {}

        super().__init__(term_map, metadata_manager,
                         callbacks=callbacks,
                         error_holder=error_holder)

        self._host = host
        self._port = port
        self._topics = topics
        self._exclude_topics = exclude_topics
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

    def datachange_notification(self, node: Node, val: [int,str,float], data: DataChangeNotification) -> None:
        # print(f"Value changed: {node.nodeid}: {val} at {data.monitored_item.Value.SourceTimestamp}")
        self._dispatch_callback(self.datachange_notification, {"node":node.nodeid.Identifier, "value":val, "timestamp":data.monitored_item.Value.SourceTimestamp, "data":data})

    def start(self) -> None:
        """
        Start the OPCWatcher
        """
        print(f"Starting OPCWatcher on {self._host}:{self._port}")
        self._client = Client(f"opc.tcp://{self._host}:{self._port}")
        self._client.connect()

        # Not sure what's going on here. Could be changed to allow user defined topics etc.
        # Are they all different measurements?
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
            nodes_data.update(self._browse_and_read(child))  # Recursive call
        return nodes_data

    def _subscribe_to_topics(self) -> None:
        """
        Subscribe to OPC UA nodes and monitor data changes.
        """
        if not self._client:
            print("Client is not connected.")
            return
        try:
            self._sub = self._client.create_subscription(1000, self)  # 1s interval
            for topic in self._topics:
                if topic in self._exclude_topics:
                    leaf.start.logger.info("Excluded topic: {}".format(topic))
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
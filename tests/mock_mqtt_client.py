import re
import json
from core.modules.output_modules.mqtt import MQTT
class MockBioreactorClient(MQTT):
    def __init__(self, broker_address, port=None, 
                 username=None,password=None):
        super().__init__(broker_address, port, 
                         username=username,password=password)
        self.messages = {}
        self.num_msg = 0
        self.client.on_message = self.on_message

    def on_message(self, client, userdata, msg):
        topic = msg.topic
        try:
            payload = msg.payload.decode()
            if payload == "":
                return
            msg = json.loads(payload)
        except json.JSONDecodeError:
            msg = msg.payload.decode()
        if topic not in self.messages:
            self.messages[topic] = []
        self.messages[topic].append(msg)
        self.num_msg += 1

    def subscribe(self, topic):
        topic = topic.strip().replace(" ", "")
        topic = re.sub(r"\s+", "", topic)
        self.client.subscribe(topic)
        return topic
    
    def unsubscribe(self,topic):
        self.client.unsubscribe(topic)
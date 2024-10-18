import re
import json
import paho.mqtt.client as mqtt
from core.modules.output_modules.mqtt import MQTT

class MockBioreactorClient(MQTT):
    def __init__(self, broker_address: str, port: int=1883,
                 username: str|None=None,password: str|None=None):
        super().__init__(broker_address, port, 
                         username=username,password=password,clientid=None)
        self.messages = {}
        self.num_msg = 0
        self.client.on_message = self.on_message

    def on_message(self, client: mqtt.Client, userdata: str, msg: str) -> None:
        topic = msg.topic
        try:
            payload = msg.payload.decode('utf-8')
            
            if payload == "":
                return

            msg = json.loads(payload)
        except UnicodeDecodeError:
            print(f"Non-UTF-8 message payload received. {topic}")
            msg = msg.payload
        except json.JSONDecodeError:
            msg = payload

        if topic not in self.messages:
            self.messages[topic] = []
            
        self.messages[topic].append(msg)
        self.num_msg += 1


    def subscribe(self, topic: str) -> str:
        topic = topic.strip().replace(" ", "")
        topic = re.sub(r"\s+", "", topic)
        self.client.subscribe(topic)
        return topic
    
    def unsubscribe(self,topic: str) -> str:
        self.client.unsubscribe(topic)
        return topic
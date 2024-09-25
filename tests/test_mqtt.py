import unittest
from core.mqtt_client import MQTTClient
from datetime import datetime
import time
import logging

logging.basicConfig(level=logging.INFO)

class TestMQTTClient(unittest.TestCase):

    def setUp(self):
        logging.info("Setting up test")
        # Initialize the MQTTClient for each test
        self.mqtt_client = MQTTClient(broker_host='test.mosquitto.org', broker_port=1883, client_id='leaf-client')
        self.mqtt_client.connect()

        self.test_time = datetime.now()
        self.received_messages = []

        # Define a custom on_message handler and assign it to the MQTT client
        def custom_on_message(client, userdata, msg):
            message_content = msg.payload.decode()
            logging.info(f"Test received message on {msg.topic}: {message_content}")
            self.received_messages.append(message_content)

        # Set the custom callback for testing on the actual Paho MQTT client
        self.mqtt_client.client.on_message = custom_on_message

        # Subscribe to the test topic
        self.mqtt_client.subscribe("leaf-test/topic")

    def test_publish(self):
        logging.info("Running test_publish")
        # Publish a test message
        test_message = f"test message {self.test_time} "
        # The publish timing might be too fast for the message to be received so we will publish it also in the else statement
        logging.info(f"MQTT client host: {self.mqtt_client.broker_host}")
        self.mqtt_client.publish("leaf-test/topic", test_message)

        # Set a timeout for waiting for the message to be received
        timeout = 5  # seconds
        start_time = time.time()

        # Wait for the message to be received or timeout
        message_received = False
        while not message_received:
            if len(self.received_messages) > 0:
                for message in self.received_messages:
                    logging.info(f"Received message: {message}")
                    # Ensure the received message is the one we sent
                    self.assertEqual(message, test_message)
                    logging.info("Test passed.")
                    message_received = True
            elif time.time() - start_time > timeout:
                self.fail(f"Timeout waiting for message to be received from {self.mqtt_client.broker_host}")
            else:
                self.mqtt_client.publish("leaf-test/topic", test_message)
            # Sleep briefly before checking again
            time.sleep(0.1)

if __name__ == '__main__':
    unittest.main()

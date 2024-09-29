import aioredis
import asyncio
import logging
import json

logging.basicConfig(level=logging.INFO)

class AsyncKeyDBClient:
    def __init__(self, host, port=6379, db=0):
        self.host = host
        self.port = port
        self.db = db
        self.client = None

    async def connect(self):
        self.client = await aioredis.create_redis_pool(
            (self.host, self.port), db=self.db
        )

    async def store_message(self, key, message):
        # Use the DBSIZE command to get the number of keys
        num_keys = await self.client.dbsize()
        print(f"Number of keys in the database: {num_keys}")
        await self.client.set(key, message)

    async def retrieve_message(self, key):
        message = await self.client.get(key)
        if message:
            return message.decode('utf-8')
        return None

    async def close(self):
        self.client.close()
        await self.client.wait_closed()

    async def run_forever(self, mqtt_client):
        # Keep polling for messages in an asynchronous loop
        while True:
            keys = await self.client.keys('*')
            logging.debug(f"Keys: {keys}")
            for key in keys:
                message = await self.retrieve_message(key.decode('utf-8'))
                # Message should be in JSON format
                if not message:
                    logging.error("No message found in the key")
                    continue
                # Check if format is JSON
                try:
                    message = json.loads(message)
                except json.JSONDecodeError:
                    logging.error("Invalid JSON message, removing key")
                    await self.client.delete(key)
                    continue
                # Topic should be in the message
                if message:
                    logging.info(f"Message: {message}")
                    topic = message['tags']['topic']
                    if not topic:
                        logging.error("No topic found in the message")
                        continue
                    # Publish to MQTT broker
                    mqtt_client.publish(topic, json.dumps(message, indent=4, sort_keys=True))
                    # Optionally remove the message after publishing
                    await self.client.delete(key)

            await asyncio.sleep(5)  # Poll every 5 seconds
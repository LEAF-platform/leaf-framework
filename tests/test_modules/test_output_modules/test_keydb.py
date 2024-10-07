import unittest
import redis
import logging

logging.basicConfig(level=logging.INFO)

class TestKeyDB(unittest.TestCase):

    def setUp(self):
        # Connect to Redis (localhost:6379 by default)
        logging.info("Connecting to KeyDB")
        self.db = redis.Redis(host='localhost', port=6379, db=0)

    def tearDown(self):
        # Clean up Redis after each test
        logging.info("Flushing KeyDB")
        self.db.flushdb()

    def test_set_and_get(self):
        # Set a key-value pair and verify that it can be retrieved
        logging.info("Setting key-value pair")
        self.db.set('key', 'value')
        logging.info("Getting key-value pair")
        result = self.db.get('key').decode('utf-8')  # Decode from bytes to string
        logging.info(f"Result: {result}")
        self.assertEqual(result, 'value')

    def test_delete(self):
        # Set a key-value pair and then delete it
        self.db.set('key', 'value')
        logging.info("Deleting key")
        self.db.delete('key')
        result = self.db.get('key')
        logging.info(f"Result: {result}")
        self.assertIsNone(result)  # Redis returns None if the key doesn't exist

    def test_flushdb(self):
        # Set multiple key-value pairs and flush the database
        logging.info("Setting key-value pairs")
        self.db.set('key1', 'value1')
        self.db.set('key2', 'value2')
        logging.info("Flushing database")
        self.db.flushdb()
        logging.info("Getting key-value pairs")
        result1 = self.db.get('key1')
        result2 = self.db.get('key2')
        logging.info(f"Result1: {result1}")
        self.assertIsNone(result1)  # Both keys should be deleted
        logging.info(f"Result2: {result2}")
        self.assertIsNone(result2)

if __name__ == '__main__':
    unittest.main()

import redis

# Connect to KeyDB
client = redis.Redis(host=HOST, port=6379, db=0)

# Storing a key-value pair (message)
def store_message(key, message):
    client.set(key, message)

# Retrieve message by key
def retrieve_message(key):
    return client.get(key).decode('utf-8')

# Example usage
store_message('msg1', 'This is the first message')
store_message('msg2', 'This is the second message')

print(retrieve_message('msg1'))  # Output: This is the first message
print(retrieve_message('msg2'))  # Output: This is the second message

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from IPython.display import clear_output
import json
import pprint
import time
import sys

# Configuration
KAFKA_BROKERS = ['kafka1:9092', 'kafka2:9092']
TOPICS = {
    'users': 'processed_users',
    'transactions': 'processed_transactions',
    'products': 'processed_products',
    'sessions': 'processed_sessions'
}

print(f"Attempting to connect to Kafka brokers: {KAFKA_BROKERS}...")
print("If this hangs, the brokers are not accessible from this environment.")

try:
    consumer = KafkaConsumer(
        *TOPICS.values(),
        bootstrap_servers=KAFKA_BROKERS,
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        # Set a timeout so we don't block indefinitely on connection
        request_timeout_ms=5000
    )
    print("✓ Connected to Kafka brokers")
except NoBrokersAvailable:
    print("\n✗ Error: Could not connect to Kafka brokers.")
    print("  Are you running this script inside the Docker network (e.g., in the Jupyter container)?")
    print("  The brokers 'kafka1' and 'kafka2' are likely not accessible from localhost.")
    sys.exit(1)
except Exception as e:
    print(f"\n✗ Error connecting to Kafka: {e}")
    sys.exit(1)

pp = pprint.PrettyPrinter(indent=4)

print(f"\nListening for messages on topics: {list(TOPICS.values())}...")
print("Waiting for data (printing '.' every second while idle)...")

try:
    msg_count = 0
    while True:
        # Use poll instead of iteration to allow for status updates
        message_batch = consumer.poll(timeout_ms=1000)
        
        if not message_batch:
            print(".", end="", flush=True)
            continue
            
        print("\n") # Newline after dots
        
        for partition, messages in message_batch.items():
            for message in messages:
                msg_count += 1
                
                print(f"Topic: {message.topic}, Partition: {message.partition}, Offset: {message.offset}")
                print(f"Key: {message.key}")
                print("Value:")
                pp.pprint(message.value)
                print("-" * 50)
                
                if msg_count % 20 == 0:
                    clear_output(wait=True)
                    print(f"Listening for messages on topics: {list(TOPICS.values())}...")
                    print("Waiting for data (printing '.' every second while idle)...")

except KeyboardInterrupt:
    print("\nStopped by user.")
finally:
    consumer.close()

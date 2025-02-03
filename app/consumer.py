from kafka import KafkaConsumer
import json
import signal
import sys

consumer = KafkaConsumer(
    'fraud_transaction',
    bootstrap_servers='localhost:9092',
    group_id='transaction_group',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Graceful shutdown handler
def signal_handler(sig, frame):
    print("Gracefully shutting down consumer...")
    consumer.close()  # Close the consumer when the script is interrupted
    sys.exit(0)

# Listen for SIGINT (Ctrl+C) and other interrupt signals to shut down gracefully
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

print("Starting the consumer...")

# Consuming messages from the topic
try:
    for message in consumer:
        print(f"Received message: {message.value}")
        # Process the transaction here (e.g., save to database, print, etc.)
except Exception as e:
    print(f"Error while consuming messages: {e}")
finally:
    consumer.close()  # Close the consumer after the loop ends or on error
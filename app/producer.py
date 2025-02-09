from kafka import KafkaProducer
import json
import time
import csv

KAFKA_BROKER_URL = "localhost:9092"
TOPIC_NAME = "fraud_transaction"

#Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers = KAFKA_BROKER_URL,
    value_serializer = lambda v: json.dumps(v).encode('utf-8')
    #Serialize messages to JSON
)

# #Dummy transaction data to simulate fraud detection
# transactions = [
#     {"transaction_id": 1, "amount": 5000, "location": "Toronto", "card_type": "credit"},
#     {"transaction_id": 2, "amount": 20, "location": "New York", "card_type": "debit"},
#     {"transaction_id": 3, "amount": 10000, "location": "San Francisco", "card_type": "credit"}
# ]

# print("Starting the producer...")

# for transaction in transactions:
#     print(f"Sending transaction: {transaction}")
#     producer.send(TOPIC_NAME, transaction) #Send the message to Kafka topic
#     time.sleep(1)

# producer.close()
# print("Producer scripts completed")

# Path to your synthetic dataset CSV
csv_file_path = 'data/real_time_data.csv'

# Function to read and stream CSV rows
def stream_data_from_csv(file_path):
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Send each row as a message to Kafka
            producer.send('transactions-topic', row)
            print(f"Produced: {row}")
            
            # Simulate real-time streaming with a delay
            time.sleep(1)

try:
    stream_data_from_csv(csv_file_path)
except KeyboardInterrupt:
    print("Streaming stopped by user.")


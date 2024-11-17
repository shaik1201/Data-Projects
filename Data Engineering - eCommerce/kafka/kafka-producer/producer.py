import requests
import json
from confluent_kafka import Producer
import time

# Kafka configuration
KAFKA_BROKER = "kafka:9092"  # Address of the Kafka broker (kafka is the hostname of the Kafka container)
KAFKA_TOPIC = "ecommerce-transactions" # Topic to which data will be published

# Configure the Kafka producer
producer_config = {
    'bootstrap.servers': KAFKA_BROKER,
    'client.id': 'python-producer', # Identifier for this producer instance
    'retry.backoff.ms': 1000,  # Time to wait before retrying in case of failures
    'message.timeout.ms': 10000  # Timeout for message delivery
}

def create_producer():
    """
    Create a Kafka producer instance with retry logic.

    Returns:
        Producer: An instance of Confluent Kafka Producer.
    """
    
    while True:
        try:
            producer = Producer(producer_config)
            return producer
        except Exception as e:
            print(f"Failed to create producer: {e}")
            print("Retrying in 5 seconds...")
            time.sleep(5)

def delivery_report(err, msg):
    """
    Callback function to handle the delivery report for a Kafka message.

    Args:
        err (KafkaError): Error details if the message delivery failed.
        msg (Message): The Kafka message that was attempted to be delivered.
    """
    
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Record successfully produced to {msg.topic()} [partition {msg.partition()}]")

def stream_data_to_kafka():
    """
    Fetch transaction data from the Flask API and send it to a Kafka topic.

    - Connects to the Flask API to retrieve transaction data.
    - Publishes the data to the specified Kafka topic.
    - Handles retries in case of network or API errors.
    """
    
    producer = create_producer()
    
    try:
        while True:
            try:
                # Fetch data from the Flask API
                response = requests.get("http://data-simulator:5000/transactions")
                if response.status_code == 200:
                    transaction_data = response.json()
                    
                    # Send data to Kafka
                    producer.produce(
                        KAFKA_TOPIC,
                        key=str(transaction_data["transaction_id"]), # Use transaction_id as the key
                        value=json.dumps(transaction_data),          # Serialize data as JSON
                        callback=delivery_report                     # Register delivery report callback
                    )
                    
                    # Flush the producer to ensure messages are sent
                    producer.flush()
                
                time.sleep(1)  # Add a small delay to control the rate of data streaming
                
            except requests.exceptions.RequestException as e:
                print(f"Error fetching data: {e}")
                time.sleep(5) 
                
    except KeyboardInterrupt:
        print("Streaming stopped.")
        producer.flush()  # Ensure any remaining messages are sent
        producer.close()  # Clean up resources

if __name__ == "__main__":
    stream_data_to_kafka()
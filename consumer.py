from confluent_kafka import Consumer, Producer, KafkaError
import json
from datetime import datetime
from collections import defaultdict
import logging
import signal
import sys
import unittest

def handle_signal(sig, frame):
    logger.info("Received termibation signal. Shutting down")
    consumer.close()
    sys.exit(0)

# Register signal handler
signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)

logging.basicConfig(level=logging.INFO)
logger=logging.getLogger(__name__)


# Kafka configuration
KAFKA_BROKER = 'localhost:29092'
INPUT_TOPIC = 'user-login'
OUTPUT_TOPIC = 'processed-user-login'
GROUP_ID = 'my-consumer-group'


message_count = defaultdict(int)


# Kafka Consumer configuration
consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest'
}
# Kafka Producer configuration
producer_conf = {
    'bootstrap.servers': KAFKA_BROKER
}

# Create Consumer and Producer instances
consumer = Consumer(consumer_conf)
producer = Producer(producer_conf)

# Subscribe to the 'user-login' topic
consumer.subscribe([INPUT_TOPIC])




def convert_timestamp(timestamp):
    """Convert Unix timestamp to human-readable format."""
    try:
        # Convert timestamp to UTC datetime string
        return datetime.utcfromtimestamp(int(timestamp)).strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        logger.info(f"Invalid timestamp: {timestamp}")
        #return None

def process_message(message):
    """Process and transform incoming message."""
    try:
        # Decode and parse the JSON message
        data = json.loads(message)

    except json.JSONDecodeError:
        logger.error("Failed to decode JSON message.")
        return None

    # Extract and convert timestamp
    timestamp = data.get("timestamp")
    if timestamp:
        data["time_in_utc"] = convert_timestamp(timestamp)
    else:
        logger.error("No timestamp found in the message.")
        return None # why is it returning None
    
    # Add a field for message processing time (in milliseconds)
    data['processed_time'] = int(datetime.utcnow().timestamp() * 1000)

    # Check for message repetition based on 'message_id'
    message_id = data.get('message_id')
    if message_id:
        message_count[message_id] += 1
        data['message_count'] = message_count[message_id]
    else:
        logger.warning("No 'message_id' found in the message. Unable to track message repetitions.")
    
    return data

    
    

def produce_message(data):
    """Produce the processed message to the output topic."""
    try:
        # Send the processed data to the new Kafka topic
        producer.produce(OUTPUT_TOPIC, json.dumps(data).encode('utf-8'))
        producer.flush()
    except Exception as e:
        logger.error(f"Error producing message: {e}")

def main():
    """Main function to consume and process messages."""
    while True:
        # Poll for new messages
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue  # No message, keep checking

        # Process the message
        processed_data = process_message(msg.value().decode('utf-8'))

        # If data is processed, send to the new topic
        if processed_data:
            logger.info(f"Sending processed data: {processed_data}")
            produce_message(processed_data)
        else:
            logger.error("Message skipped due to processing error.")



"""
class TestKafkaConsumer(unittest.TestCase):

    def test_process_message_valid(self):
        message = '{"message_id": "1", "timestamp": "1609459200"}'
        result = process_message(message)
        self.assertIsNotNone(result)
        self.assertIn('processed_time', result)

    def test_process_message_invalid_json(self):
        message = '{"message_id": "1", "timestamp": 1609459200'  # Invalid JSON
        result = process_message(message)
        self.assertIsNone(result)

    def test_process_message_no_timestamp(self):
        message = '{"message_id": "1"}'
        result = process_message(message)
        self.assertIsNone(result)

if __name__ == '__main__':
    unittest.main()

"""

if __name__ == "__main__":
    main()

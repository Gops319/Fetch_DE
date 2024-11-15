from confluent_kafka import Consumer, Producer, KafkaError
from kafka import KafkaProducer
import json
from datetime import datetime
import logging
import signal
import sys
import unittest
from unittest.mock import patch

class KafkaProcessor:
    def __init__(self, consumer_conf, producer_conf, input_topic, output_topic):
        self.consumer_conf = consumer_conf
        self.producer_conf = producer_conf
        self.input_topic = input_topic
        self.output_topic = output_topic

        self.consumer = Consumer(self.consumer_conf)
        self.producer = Producer(self.producer_conf)

        self.unique_messages = set()
        self.total_messages = 0
        self.unique_message_count = 0

        # Set up logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        # Register signal handler
        signal.signal(signal.SIGTERM, self.handle_signal)
        signal.signal(signal.SIGINT, self.handle_signal)

    def handle_signal(self, sig, frame):
        """Handle termination signal."""
        self.logger.info("Received termination signal. Shutting down.")
        self.consumer.close()
        sys.exit(0)

    def process_message(self, message):
        """Process the Kafka message."""
        try:
            data = json.loads(message)
            timestamp=data.get("timestamp")

            if timestamp is None:
                return None
            # self.total_messages += 1

            # Generate a hash for the message (use sorted JSON to ensure consistency)
            # message_hash = hashlib.md5(json.dumps(data, sort_keys=True).encode('utf-8')).hexdigest()

            # Check if the message is unique
            # if message_hash not in self.unique_messages:
            #     self.unique_messages.add(message_hash)
            #     self.unique_message_count += 1  # Increment unique message count

            # Add processed_time to the message
            data['time_in_utc'] = datetime.utcfromtimestamp(int(timestamp)).strftime('%Y-%m-%d %H:%M:%S')
            data['processed_time'] = int(datetime.utcnow().timestamp() * 1000)

            return data
        except json.JSONDecodeError:
            self.logger.error("Failed to decode message.")
        return None

    def produce_message(self, data):
        """Produce the processed message to the output topic."""
        try:
            # Send the processed data to the new Kafka topic
            self.producer.produce(self.output_topic, json.dumps(data).encode('utf-8'))
            self.producer.flush()
        except Exception as e:
            self.logger.error(f"Error producing message: {e}")

    def consume_messages(self):
        """Consume messages from the Kafka topic and process them."""
        while True:
            # Poll for new messages
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    self.logger.info(f"Error: {msg.error()}")
                    break

            # Process the message
            processed_data = self.process_message(msg.value().decode('utf-8'))

            # If data is processed, send to the new topic
            if processed_data:
                self.logger.info(f"Sending processed data: {processed_data}")
                self.produce_message(processed_data)
            else:
                self.logger.error("Message skipped due to processing error.")

    def start(self):
        """Start the Kafka consumer."""
        self.consumer.subscribe([self.input_topic])
        self.consume_messages()

# Unit Test Class
class TestKafkaProcessor(unittest.TestCase):
    def setUp(self):
        """Set up a KafkaProcessor instance for testing."""
        consumer_conf = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'my-consumer-group',
            'auto.offset.reset': 'earliest'
        }
        producer_conf = {
            'bootstrap.servers': 'localhost:9092'
        }
        self.kafka_processor = KafkaProcessor(consumer_conf, producer_conf, 'input-topic', 'output-topic')

    def test_process_message_valid(self):
        """Test that process_message works with valid input."""
        message = '{"message_id": "1", "timestamp": "1609459200"}'  # Valid JSON
        result = self.kafka_processor.process_message(message)
        self.assertIsNotNone(result)
        self.assertIn('processed_time', result)
        self.assertIn('time_in_utc', result)

        # Check if the 'processed_time' is in correct format
        try:
            processed_time = datetime.utcfromtimestamp(result['processed_time'] / 1000)
            self.assertIsInstance(processed_time, datetime)
        except Exception as e:
            self.fail(f"processed_time format is incorrect: {e}")

    def test_process_message_invalid_json(self):
        """Test that process_message returns None for invalid JSON."""
        message = '{"message_id": "1", "timestamp": 1609459200'  # Invalid JSON (missing closing bracket)
        result = self.kafka_processor.process_message(message)
        self.assertIsNone(result)

    def test_process_message_no_timestamp(self):
        """Test that process_message returns None if timestamp is missing."""
        message = '{"message_id": "1"}'  # Missing timestamp field
        result = self.kafka_processor.process_message(message)
        self.assertIsNone(result)

    @patch.object(KafkaProducer, 'send', return_value = None)
    def test_produce_message(self, mock_produce):
        """Test that produce_message works and calls the produce method."""
        data = {"message_id": "1", "processed_time": 1609459200}
        self.kafka_processor.produce_message(data)
        mock_produce.assert_called_once_with('output-topic', json.dumps(data).encode('utf-8'))

if __name__ == "__main__":
    # Kafka consumer configuration
    consumer_conf = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'my-consumer-group',
        'auto.offset.reset': 'earliest'
    }

    # Kafka producer configuration
    producer_conf = {
        'bootstrap.servers': 'kafka:9092'
    }

    INPUT_TOPIC = 'user-login'
    OUTPUT_TOPIC = 'processed-user-login'

    # Create KafkaProcessor instance and start consuming messages
    kafka_processor = KafkaProcessor(consumer_conf, producer_conf, INPUT_TOPIC, OUTPUT_TOPIC)
    #kafka_processor.start()

    # Run the tests
    unittest.main(argv=[''], verbosity=2, exit=False)
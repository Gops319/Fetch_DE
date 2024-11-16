import unittest
from unittest.mock import patch
from confluent_kafka import Consumer, Producer, KafkaError
import json
from datetime import datetime
from consumer import KafkaProcessor

# Unit Test Class
class TestKafkaProcessor(unittest.TestCase):
    def setUp(self):
        """Set up a KafkaProcessor instance for testing."""
        consumer_conf = {
            'bootstrap.servers': 'kafka:9092',
            'group.id': 'my-consumer-group',
            'auto.offset.reset': 'earliest'
        }
        producer_conf = {
            'bootstrap.servers': 'kafka:9092'
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

    @patch.object(Producer, 'send', return_value = None)
    def test_produce_message(self, mock_produce):
        """Test that produce_message works and calls the produce method."""
        data = {"message_id": "1", "processed_time": 1609459200}
        self.kafka_processor.produce_message(data)
        mock_produce.assert_called_once_with('output-topic', json.dumps(data).encode('utf-8'))

# Run the tests
unittest.main(argv=[''], verbosity=2, exit=False)
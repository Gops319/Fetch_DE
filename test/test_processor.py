import unittest
import sys
from datetime import datetime
from processor import KafkaProcessor

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
        self.kafka_processor = KafkaProcessor(consumer_conf, producer_conf, 'input-topic', 'output-topic', 'device_type_count')
        

    def test_process_message_valid(self):
        """Test that process_message works with valid input."""
        message = '{"user_id": "9507120b-9c7f-49ab-b424-a091e674ab07", "app_version": "2.3.0", "ip": "106.199.123.247", "locale": "SC", "device_id": "d9fe4eb9-321f-44cc-86d8-127a8733d00b", "timestamp": 1731804358, "device_type": "iOS"}'  # Valid JSON
        result = self.kafka_processor.process_message(message)
        self.assertIsNotNone(result)
        self.assertIn('processed_time', result)
        self.assertIn('timestamp_in_utc', result)

        # Check if the 'processed_time' is in correct format
        
        processed_time = datetime.utcfromtimestamp(result['processed_time'] / 1000)
        self.assertIsInstance(processed_time, datetime)

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

# Run the tests
unittest.main(argv=[''], verbosity=2, exit=False)
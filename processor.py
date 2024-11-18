from confluent_kafka import Consumer, Producer, KafkaError
import json
from datetime import datetime
import logging
import signal
import sys
from collections import defaultdict
import time

class KafkaProcessor:
    def __init__(self, consumer_conf, producer_conf, input_topic, output_topic, aggregated_topic):
        
        time.sleep(10) # Delay to ensure the consumer doesn't start before the producer creates the topics
        self.consumer_conf = consumer_conf
        self.producer_conf = producer_conf
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.aggregated_topic = aggregated_topic

        # Kafka consumer and producer instances
        self.consumer = Consumer(self.consumer_conf)
        self.producer = Producer(self.producer_conf)

        self.device_type_counts = defaultdict(int)
        self.last_report_time = time.time()


        # Set up logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        # Register signal handler to ensure graceful shutdown
        signal.signal(signal.SIGTERM, self.handle_signal)
        signal.signal(signal.SIGINT, self.handle_signal)


    def handle_signal(self, sig, frame):
        """Handle termination signal."""
        self.logger.info("Received termination signal. Shutting down.")
        self.consumer.close()
        sys.exit(0)

    def process_message(self, message):
        """Process the Kafka message."""
        
        # write try and exception for this
        try:
            data = json.loads(message)
        except:
            return None # Return None if the message is not valid JSON

        # Skip the messages if 'device_type' and 'app_version' fields are missing
        if 'device_type' not in data or 'app_version' not in data:
            self.logger.warning("Skipping message due to missing keys.")
            return None
        
        # Only allow 'android' or 'iOS' as device types
        if data['device_type'].lower() not in ['android', 'ios']:
            return None
        
        # Skip messages with app version lower than '2.3.0'
        if data['app_version'] < '2.3.0':
            return None
        
        # Ensure that a timestamp is provided
        if data['timestamp'] is None:
            return None

        # Add processed_time to the message and convert message timestamp to UTC
        data['timestamp_in_utc'] = datetime.utcfromtimestamp(int(data['timestamp'])).strftime('%Y-%m-%d %H:%M:%S')
        data['processed_time'] = int(datetime.utcnow().timestamp() * 1000)

        return data


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
            msg = self.consumer.poll(timeout=1.0) # Set a timeout to avoid blocking indefinitely
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue # Ignore end of partition errors
                else:
                    self.logger.info(f"Error: {msg.error()}")
                    break

            # Process the message
            processed_data = self.process_message(msg.value().decode('utf-8'))

            # If data is processed, send to the new topic
            if processed_data:
                self.logger.info(f"Sending processed data: {processed_data}")
                self.aggregate_data(processed_data)
                self.produce_message(processed_data)
            else:
                self.logger.error("Message skipped due to processing error.")
            
            # Periodically report counts
            self.produce_aggregated_data()

    def aggregate_data(self, data):
        """Perform real-time aggregation."""

        # Increment device type counts
        self.device_type_counts[data['device_type']] += 1


    def produce_aggregated_data(self):
        """Periodically produce the aggregation results."""
        
        current_time = time.time()
        if current_time - self.last_report_time >= 60:  # Report every 60 seconds
            aggregation_data = {"device_type_counts": dict(self.device_type_counts)}

            self.producer.produce(self.aggregated_topic, json.dumps(aggregation_data).encode('utf-8'))
            self.producer.flush()
            self.last_report_time = current_time # Reset the report time


    def start(self):
        """Start the Kafka consumer."""
        self.consumer.subscribe([self.input_topic])
        self.consume_messages()

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
    AGGREGATED_TOPIC = 'device_type_count'

    
    
    # Create KafkaProcessor instance and start consuming messages
    kafka_processor = KafkaProcessor(consumer_conf, producer_conf, INPUT_TOPIC, OUTPUT_TOPIC, AGGREGATED_TOPIC)
    kafka_processor.start()

    
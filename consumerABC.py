import abc
import json
from confluent_kafka import Consumer
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from fastavro import parse_schema

class AbstractKafkaConsumer(abc.ABC):
    """
    Abstract base class for Kafka consumers.

    This class provides a framework for consuming messages from a Kafka topic,
    deserializing them using Avro, and processing them in batches.
    Subclasses must implement the `process_message` and `handle_batch` methods.

    Attributes:
        kafka_config (dict): Configuration for the Kafka consumer.
        schema_registry_conf (dict): Configuration for the Schema Registry client.
        topic (str): Kafka topic to consume messages from.
        subject_name (str): Subject name for the Avro schema in the Schema Registry.
    """

    def __init__(self, kafka_config, schema_registry_conf, topic, subject_name):
        """
        Initializes the AbstractKafkaConsumer with the provided configurations.

        Args:
            kafka_config (dict): Configuration for the Kafka consumer.
            schema_registry_conf (dict): Configuration for the Schema Registry client.
            topic (str): Kafka topic to consume messages from.
            subject_name (str): Subject name for the Avro schema in the Schema Registry.
        """
        self.kafka_config = kafka_config
        self.schema_registry_conf = schema_registry_conf
        self.subject_name = subject_name
        self.topic = topic

    @abc.abstractmethod
    def process_message(self, value):
        """
        Abstract method to process a single message.

        Subclasses must implement this method to define how to process each message.

        Args:
            value (dict): Deserialized message value.
        """
        pass

    def consume_messages(self, batch_size=1000):
        """
        Consumes messages from the Kafka topic, processes them in batches, and handles errors.

        This method sets up the Kafka consumer, subscribes to the topic, retrieves the schema
        from the Schema Registry, and deserializes the messages using Avro. It processes messages
        in batches and calls `process_message` for each message and `handle_batch` for each batch.

        Args:
            batch_size (int, optional): Number of messages to process in each batch. Defaults to 1000.
        """
        self.consumer = Consumer(self.kafka_config)
        self.consumer.subscribe([self.topic])

        self.schema_registry_client = SchemaRegistryClient(self.schema_registry_conf)
        self.schema_str = self.schema_registry_client.get_latest_version(self.subject_name).schema.schema_str
        self.parsed_schema = parse_schema(json.loads(self.schema_str))

        self.deserializer = AvroDeserializer(self.schema_registry_client, self.schema_str)

        record_batch = []
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                value = self.deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
                record_batch.append(value)
                self.process_message(value)

                if len(record_batch) >= batch_size:
                    self.handle_batch(record_batch)
                    record_batch.clear()
        except KeyboardInterrupt:
            pass
        finally:
            if record_batch:
                self.handle_batch(record_batch)
            self.consumer.close()

    @abc.abstractmethod
    def handle_batch(self, record_batch):
        """
        Abstract method to handle a batch of messages.

        Subclasses must implement this method to define how to handle each batch of messages.

        Args:
            record_batch (list): List of deserialized message values.
        """
        pass

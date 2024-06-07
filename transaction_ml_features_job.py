import json
import os
import redis
from fastavro import writer, parse_schema
from consumerABC import AbstractKafkaConsumer
from confluent_kafka import KafkaError, KafkaException

class TransactionsMLFeaturesJobConsumer(AbstractKafkaConsumer):
    """
    Kafka consumer for processing transaction data and generating machine learning features.

    This consumer aggregates transaction counts for each user and writes the results to Avro files.

    Attributes:
        kafka_config (dict): Configuration for the Kafka consumer.
        schema_registry_conf (dict): Configuration for the Schema Registry client.
        topic (str): Kafka topic to consume messages from.
        subject_name (str): Subject name for the Avro schema in the Schema Registry.
        output_dir (str): Directory to store output Avro files.
        redis_host (str): Redis host address.
        redis_port (int): Redis port number.
        backfill_mode (bool): Flag indicating whether the consumer is in backfill mode.
    """

    def __init__(self, kafka_config, schema_registry_conf, topic, subject_name, output_dir, redis_host='localhost', redis_port=6379, backfill_mode=False):
        """
        Initializes the TransactionsMLFeaturesJobConsumer.

        Args:
            kafka_config (dict): Configuration for the Kafka consumer.
            schema_registry_conf (dict): Configuration for the Schema Registry client.
            topic (str): Kafka topic to consume messages from.
            subject_name (str): Subject name for the Avro schema in the Schema Registry.
            output_dir (str): Directory to store output Avro files.
            redis_host (str, optional): Redis host address. Defaults to 'localhost'.
            redis_port (int, optional): Redis port number. Defaults to 6379.
            backfill_mode (bool, optional): Flag indicating whether the consumer is in backfill mode. Defaults to False.
        """
        super().__init__(kafka_config, schema_registry_conf, topic, subject_name)
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self.backfill_mode = backfill_mode

        # Initialize Redis client if not in backfill mode
        if not backfill_mode:
            self.redis_client = redis.StrictRedis(host=self.redis_host, port=self.redis_port, db=0)

        # Schema for output Avro files
        self.output_schema_str = json.dumps({
            "type": "record",
            "name": "TransactionCount",
            "fields": [
                {"name": "user_id", "type": "int"},
                {"name": "total_transactions_count", "type": "int"}
            ]
        })
        self.output_schema = parse_schema(json.loads(self.output_schema_str))

    def process_message(self, value):
        """
        Process a single Kafka message.

        Increments the total transaction count for the user in Redis if not in backfill mode.

        Args:
            value (dict): Deserialized Kafka message value.
        """
        user_id = value['user_id']
        if not self.backfill_mode:
            self.redis_client.hincrby('transactions', user_id, 1)

    def handle_batch(self, record_batch):
        """
        Process a batch of Kafka messages.

        Aggregates transaction counts for each user and writes the results to Avro files.

        Args:
            record_batch (list): List of deserialized Kafka message values.
        """
        # Aggregate transaction counts
        user_transaction_counts = {}
        for record in record_batch:
            user_id = record['user_id']
            if user_id in user_transaction_counts:
                user_transaction_counts[user_id] += 1
            else:
                user_transaction_counts[user_id] = 1

        # Prepare records for output
        output_records = [{'user_id': user_id, 'total_transactions_count': count}
                          for user_id, count in user_transaction_counts.items()]

        # Write to Avro file
        file_path = os.path.join(self.output_dir, f'transactions_{len(os.listdir(self.output_dir))}.avro')
        with open(file_path, 'wb') as out:
            writer(out, self.output_schema, output_records)

    def backfill(self):
        """
        Perform backfilling by consuming messages from the beginning of the Kafka topic.

        This method sets the consumer to start from the earliest offset and processes all messages,
        updating the Redis counts accordingly.
        """
        # Set consumer to start from the earliest offset
        self.consumer.assign([self.consumer.assignment()[0].offset(0)])
        while True:
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                break  # No more messages
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                else:
                    raise KafkaException(msg.error())
            record = msg.value()
            self.process_message(record)
        
        # Process remaining messages
        self.handle_batch(self.consumer)

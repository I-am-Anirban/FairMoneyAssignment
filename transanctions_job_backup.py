import os
from fastavro import writer

from consumerABC import AbstractKafkaConsumer

class TransactionsJobBackupConsumer(AbstractKafkaConsumer):
    """
    Kafka consumer for backing up transaction messages to Avro files.

    This consumer writes received Kafka messages to Avro files in the specified output directory.

    Attributes:
        kafka_config (dict): Configuration for the Kafka consumer.
        schema_registry_conf (dict): Configuration for the Schema Registry client.
        topic (str): Kafka topic to consume messages from.
        subject_name (str): Subject name for the Avro schema in the Schema Registry.
        output_dir (str): Directory to store output Avro files.
    """

    def __init__(self, kafka_config, schema_registry_conf, topic, subject_name, output_dir):
        """
        Initializes the TransactionsJobBackupConsumer.

        Args:
            kafka_config (dict): Configuration for the Kafka consumer.
            schema_registry_conf (dict): Configuration for the Schema Registry client.
            topic (str): Kafka topic to consume messages from.
            subject_name (str): Subject name for the Avro schema in the Schema Registry.
            output_dir (str): Directory to store output Avro files.
        """
        super().__init__(kafka_config, schema_registry_conf, topic, subject_name)
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def process_message(self, value):
        """
        Process a single Kafka message.

        This method can be implemented to perform any per-message processing if needed.

        Args:
            value (dict): Deserialized Kafka message value.
        """
        pass  # Implement any per-message processing if needed

    def handle_batch(self, record_batch):
        """
        Process a batch of Kafka messages and write them to Avro files.

        Args:
            record_batch (list): List of deserialized Kafka message values.
        """
        # Generate a unique file name based on the number of existing files in the output directory
        file_path = os.path.join(self.output_dir, f'transactions_{len(os.listdir(self.output_dir))}.avro')
        
        # Write the record batch to the Avro file
        with open(file_path, 'wb') as out:
            writer(out, self.parsed_schema, record_batch)


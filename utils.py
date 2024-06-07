import json
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from fastavro import parse_schema


class KafkaConfig:
    """
    Configuration class for Kafka and Schema Registry.

    This class encapsulates the configuration parameters required for interacting with Kafka and Schema Registry.

    Attributes:
        bootstrap_servers (str): Comma-separated list of Kafka broker addresses.
        schema_registry_url (str): URL of the Schema Registry.
        group_id (str, optional): Consumer group ID. Defaults to None.
    """

    def __init__(self, bootstrap_servers, schema_registry_url, group_id=None):
        """
        Initializes the KafkaConfig object.

        Args:
            bootstrap_servers (str): Comma-separated list of Kafka broker addresses.
            schema_registry_url (str): URL of the Schema Registry.
            group_id (str, optional): Consumer group ID. Defaults to None.
        """
        self.kafka_config = {'bootstrap.servers': bootstrap_servers}
        self.schema_registry_conf = {'url': schema_registry_url}
        self.group_id = group_id
        if group_id:
            self.kafka_config['group.id'] = group_id
            self.kafka_config['auto.offset.reset'] = 'earliest'


class SchemaRegistry:
    """
    Wrapper class for interacting with Schema Registry.

    This class provides methods for registering schemas and obtaining serializers/deserializers.

    Attributes:
        schema_registry_client (SchemaRegistryClient): Schema Registry client instance.
        schema_str (str): String representation of the Avro schema.
        schema (Schema): Schema object representing the Avro schema.
        schema_id (int): ID of the registered schema in Schema Registry.
        parsed_schema (dict): Parsed Avro schema as a dictionary.
    """

    def __init__(self, schema_registry_conf, schema_file):
        """
        Initializes the SchemaRegistry object.

        Args:
            schema_registry_conf (dict): Configuration for the Schema Registry client.
            schema_file (str): Path to the Avro schema file.
        """
        self.schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        
        # Read the Avro schema from the file
        with open(schema_file, 'r') as f:
            self.schema_str = f.read()
        
        # Create a Schema object
        self.schema = Schema(self.schema_str, schema_type='AVRO')
        
        # Register the schema with Schema Registry and obtain its ID
        self.schema_id = self.schema_registry_client.register_schema("transactions-value", self.schema)
        
        # Parse the Avro schema into a dictionary
        self.parsed_schema = parse_schema(json.loads(self.schema_str))

    def get_serializer(self):
        """
        Get an Avro serializer for the registered schema.

        Returns:
            AvroSerializer: Avro serializer instance.
        """
        return AvroSerializer(self.schema_registry_client, self.schema_str)

    def get_deserializer(self):
        """
        Get an Avro deserializer for the registered schema.

        Returns:
            AvroDeserializer: Avro deserializer instance.
        """
        return AvroDeserializer(self.schema_registry_client, self.schema_str)

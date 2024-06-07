from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from utils import SchemaRegistry

class TransactionsProducer:
    """
    Kafka producer for generating transaction messages.

    This producer sends transaction messages to a Kafka topic, utilizing Avro serialization.

    Attributes:
        kafka_config (dict): Configuration for the Kafka producer.
        schema_registry_conf (dict): Configuration for the Schema Registry client.
        producer_schema_file (str): Path to the Avro schema file for message serialization.
        topic (str): Kafka topic to produce messages to.
    """

    def __init__(self, kafka_config, schema_registry_conf, producer_schema_file, topic):
        """
        Initializes the TransactionsProducer.

        Args:
            kafka_config (dict): Configuration for the Kafka producer.
            schema_registry_conf (dict): Configuration for the Schema Registry client.
            producer_schema_file (str): Path to the Avro schema file for message serialization.
            topic (str): Kafka topic to produce messages to.
        """
        self.kafka_config = kafka_config
        self.schema_registry_conf = schema_registry_conf
        self.schema_file = producer_schema_file
        self.topic = topic

    def _get_num_partitions(self):
        """
        Retrieve the number of partitions for the Kafka topic.

        Returns:
            int: Number of partitions for the topic.
        
        Raises:
            ValueError: If the topic does not exist.
        """
        metadata = self.producer.list_topics(self.topic)
        topic_metadata = metadata.topics.get(self.topic)
        if topic_metadata:
            return len(topic_metadata.partitions)
        else:
            raise ValueError(f"Topic {self.topic} does not exist")

    def delivery_report(self, err, msg):
        """
        Delivery report callback for message delivery status.

        Args:
            err (KafkaError): Delivery error, if any.
            msg (Message): Delivered message object.
        """
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def send_message(self, value, value_serializer):
        """
        Send a single message to the Kafka topic.

        Args:
            value (dict): Message value to be serialized and sent.
            value_serializer (callable): Serializer function for the message value.
        """
        try:
            serialized_value = value_serializer(value, SerializationContext(self.topic, MessageField.VALUE))
            key = str(value['user_id'])
            num_partitions = self._get_num_partitions()
            partition = value['user_id'] % num_partitions
            self.producer.produce(topic=self.topic, key=key, value=serialized_value, partition=partition, on_delivery=self.delivery_report)
            self.producer.flush()
        except Exception as e:
            print(f"Message serialization failed: {e}")

    def producer_process(self, start_user_id, num_messages):
        """
        Generate and send multiple messages to the Kafka topic.

        Args:
            start_user_id (int): Starting user ID for message generation.
            num_messages (int): Number of messages to generate and send.
        """
        self.producer = Producer(self.kafka_config)
        self.schema_registry = SchemaRegistry(self.schema_registry_conf, self.schema_file)
        value_serializer = self.schema_registry.get_serializer()
        for i in range(num_messages):
            user_id = start_user_id + i
            value = {
                'user_id': user_id,
                'transaction_timestamp_millis': 1622547800000,
                'amount': 150.75,
                'currency': 'USD',
                'counterpart_id': 2
            }
            self.send_message(value, value_serializer)

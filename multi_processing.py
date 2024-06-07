import multiprocessing
from transaction_ml_features_job import TransactionsMLFeaturesJobConsumer
from transactions_producer import TransactionsProducer
from transanctions_job_backup import TransactionsJobBackupConsumer

def start_producers(kafka_config, schema_registry_conf, topic, start_user_id, num_messages, num_producers, producer_schema_file):
    """
    Start multiple instances of a Kafka producer to send messages to a Kafka topic.

    Args:
        kafka_config (dict): Configuration for the Kafka producer.
        schema_registry_conf (dict): Configuration for the Schema Registry client.
        topic (str): Kafka topic to send messages to.
        start_user_id (int): Starting user ID for message generation.
        num_messages (int): Number of messages each producer should send.
        num_producers (int): Number of producer instances to start.
        producer_schema_file (str): Path to the Avro schema file for the producer.
    """
    processes = []
    
    for i in range(num_producers):
        # Initialize the producer with the provided configurations and schema file
        kafkaProducer = TransactionsProducer(kafka_config, schema_registry_conf, producer_schema_file, topic)
        # Create a new process for the producer process
        p = multiprocessing.Process(
            target=kafkaProducer.producer_process,
            args=(start_user_id + i * num_messages, num_messages)
        )
        processes.append(p)
        p.start()  # Start the producer process

    # Wait for all producer processes to finish
    for p in processes:
        p.join()

    print("All messages sent successfully!")

def start_consumers(kafka_config, schema_registry_conf, topic, subject_name, output_dir, num_consumers):
    """
    Start multiple instances of a Kafka consumer to consume messages from a Kafka topic.

    Args:
        kafka_config (dict): Configuration for the Kafka consumer.
        schema_registry_conf (dict): Configuration for the Schema Registry client.
        topic (str): Kafka topic to consume messages from.
        subject_name (str): Subject name for the Avro schema in the Schema Registry.
        output_dir (str): Directory to store output files.
        num_consumers (int): Number of consumer instances to start.
    """
    processes = []
    
    for _ in range(num_consumers):
        # Initialize the consumer with the provided configurations
        kafkaConsumer = TransactionsJobBackupConsumer(kafka_config, schema_registry_conf, topic, subject_name, output_dir)
        # Create a new process for the consumer process
        p = multiprocessing.Process(target=kafkaConsumer.consume_messages)
        p.start()  # Start the consumer process
        processes.append(p)

    # Wait for all consumer processes to finish
    for p in processes:
        p.join()

    print("All messages consumed and processed successfully!")

def start_consumers_ml(kafka_config, schema_registry_conf, topic, subject_name, output_dir, num_consumers):
    """
    Start multiple instances of a Kafka consumer for machine learning feature extraction.

    Args:
        kafka_config (dict): Configuration for the Kafka consumer.
        schema_registry_conf (dict): Configuration for the Schema Registry client.
        topic (str): Kafka topic to consume messages from.
        subject_name (str): Subject name for the Avro schema in the Schema Registry.
        output_dir (str): Directory to store output files.
        num_consumers (int): Number of consumer instances to start.
    """
    processes = []
    
    for _ in range(num_consumers):
        # Initialize the consumer with the provided configurations
        kafkaConsumer = TransactionsMLFeaturesJobConsumer(kafka_config, schema_registry_conf, topic, subject_name, output_dir)
        # Create a new process for the consumer process
        p = multiprocessing.Process(target=kafkaConsumer.consume_messages)
        p.start()  # Start the consumer process
        processes.append(p)

    # Wait for all consumer processes to finish
    for p in processes:
        p.join()

    print("All messages consumed and processed successfully!")

from multi_processing import start_consumers, start_consumers_ml, start_producers
from utils import KafkaConfig, SchemaRegistry
import json

if __name__ == "__main__":
    # Kafka and Schema Registry configurations
    with open('config.json', 'r') as f:
        config = json.load(f)

    # for producing data to topic
    producer_kafka_config = KafkaConfig(config['kafka']['bootstrap_servers'], config['schema_registry']['url'])

    start_producers(producer_kafka_config.kafka_config, producer_kafka_config.schema_registry_conf, 
                    config['producer']['topic'], config['producer']['start_user_id'], config['producer']['num_messages'],
                    config['producer']['num_producers'], config['producer']['schema_file'])
    
    # transaction backup service
    transaction_backup_consumer_kafka_config = KafkaConfig(config['kafka']['bootstrap_servers'], config['schema_registry']['url'], 
                                         'transactions-job-backup')
    start_consumers(transaction_backup_consumer_kafka_config.kafka_config, 
                    transaction_backup_consumer_kafka_config.schema_registry_conf, 
                    config['consumer']['transaction_job_backup']['topic'],
                    config['consumer']['transaction_job_backup']['subject_name'],
                    config['consumer']['transaction_job_backup']['output_dir'],
                    config['consumer']['transaction_job_backup']['num_consumers'])

    #transaction ml features service
    transaction_ml_features_consumer_kafka_config = KafkaConfig(config['kafka']['bootstrap_servers'], config['schema_registry']['url'], 
                                         'transactions-ml-features')
    
    start_consumers_ml(transaction_ml_features_consumer_kafka_config.kafka_config,
                       transaction_ml_features_consumer_kafka_config.schema_registry_conf,
                       config['consumer']['transaction_ml_features']['topic'],
                    config['consumer']['transaction_ml_features']['subject_name'],
                    config['consumer']['transaction_ml_features']['output_dir'],
                    config['consumer']['transaction_ml_features']['num_consumers'])
    
    


    



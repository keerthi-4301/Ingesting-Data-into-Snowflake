import os
import logging
import sys
import confluent_kafka #This is the Python client library to interact with Kafka (used for producing messages to Kafka topics).
from kafka.admin import KafkaAdminClient, NewTopic #These are used to create new Kafka topics if they don’t already exist.

from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)

kafka_brokers = os.getenv("REDPANDA_BROKERS") #This stores the address of the Kafka broker (e.g., 127.0.0.1:19092), where messages will be sent.
topic_name = os.getenv("KAFKA_TOPIC") #This stores the name of the Kafka topic (e.g., TESTING), where messages will be published.

#creating Kafka topic
def create_topic():
    admin_client = KafkaAdminClient(bootstrap_servers=kafka_brokers, client_id='publish_data')
    topic_metadata = admin_client.list_topics()
    if topic_name not in topic_metadata:
        topic = NewTopic(name=topic_name, num_partitions=10, replication_factor=1)
        admin_client.create_topics(new_topics=[topic], validate_only=False)

#KafkaAdminClient: This creates an admin client that can manage Kafka topics (create, delete, etc.).
#list_topics(): This function retrieves a list of all existing Kafka topics on the broker.
#NewTopic: If the topic doesn't exist, this line creates a new topic with the specified name, 10 partitions, and a replication factor of 1.
#create_topics(): This creates the new topic in Kafka.

#getting kafka producer
def get_kafka_producer():
    logging.info(f"Connecting to kafka")
    config = {'bootstrap.servers': kafka_brokers}
    return confluent_kafka.Producer(**config) #confluent_kafka.Producer: This creates a Kafka producer, which is responsible for sending messages to the Kafka topic.


if __name__ == "__main__":  
    producer = get_kafka_producer()
    create_topic()
    for message in sys.stdin:
        if message != '\n':
            failed = True
            while failed:
                try:
                    producer.produce(topic_name, value=bytes(message, encoding='utf8'))
                    failed = False
                except BufferError as e:
                    producer.flush()
                
        else:
            break
    producer.flush()
import json
import logging
import boto3
import time
from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
from aws_schema_registry import SchemaRegistryClient, DataAndSchema
from aws_schema_registry.adapter.kafka import KafkaDeserializer

try:
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    AWS_REGION = "ap-south-1"
    SCHEMA_REGISTRY_NAME = "mastodon"
    topic_name = "post_data"


    session = boto3.Session(region_name=AWS_REGION)
    glue_client = session.client("glue")
    client = SchemaRegistryClient(glue_client, registry_name=SCHEMA_REGISTRY_NAME)
    deserializer = KafkaDeserializer(client)

    consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=["b-1.kafkaprojectcluster.h6naec.c4.kafka.ap-south-1.amazonaws.com:9092","b-2.kafkaprojectcluster.h6naec.c4.kafka.ap-south-1.amazonaws.com:9092"],
            group_id='post_consumer_group',
            value_deserializer=deserializer,
            auto_offset_reset='latest',
            enable_auto_commit=False
        )

    FIREHOSE_DELIVERY_STREAM = 'PUT-S3-6n1Yr'
    firehose_client = boto3.client('firehose', region_name='ap-south-1')
    logger.info("Configurations done")
    
except Exception as e:
    logger.error(f"Failed config{e}")

def send_to_firehose(data):
    try:
        response = firehose_client.put_record(
            DeliveryStreamName=FIREHOSE_DELIVERY_STREAM,
            Record={'Data': json.dumps(data) + '\n'}
        )
        logger.info(f"Sent to Firehose: {response}")
        return True
    except Exception as e:
        logger.error(f"Failed to send to Firehose: {e}")
        time.sleep(5)
    return False

def consume_messages():
    while True:
        try:
            for message in consumer:
                value: DataAndSchema = message.value
                data, schema = value
                if send_to_firehose(data):
                
                    tp = TopicPartition(message.topic, message.partition) 
                    om = OffsetAndMetadata(message.offset+1,message.timestamp)
                    consumer.commit({tp:om})
                    logger.info(f"Committed offset: {message.offset + 1} for partition: {message.partition}")
            logger.info("status active running")
        except Exception as e:
            logger.error(f"Failed to send to Firehose: {e}")
            time.sleep(2)

        
if __name__ == "__main__":
    consume_messages()
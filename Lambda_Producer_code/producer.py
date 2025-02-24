# Have to add 2 layers check my layers folder for zip files upload that to lambda layers 
import json
import logging
from kafka import KafkaProducer
import boto3
from aws_schema_registry import SchemaRegistryClient
from aws_schema_registry.avro import AvroSchema
from aws_schema_registry.adapter.kafka import KafkaSerializer

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BOOTSTRAP_SERVERS = ["broker1_endpoint:9094","broker2_endpoint:9094"] # If more broker ass here also 9094 is for TLS Encryption use 9092 if using plaintext and unauthorized access method for project
TOPIC_NAME = "post_data"

AWS_REGION = "ap-south-1"
SCHEMA_REGISTRY_NAME = "mastodon"
SCHEMA_NAME = "post_schema" # keep anything just create a schema here and keep it ready 


session = boto3.Session(region_name=AWS_REGION)
glue_client = session.client("glue")
client = SchemaRegistryClient(glue_client, registry_name=SCHEMA_REGISTRY_NAME)
serializer = KafkaSerializer(client)


response = glue_client.get_schema_version(
    SchemaId={"SchemaName": SCHEMA_NAME, "RegistryName": SCHEMA_REGISTRY_NAME},
    SchemaVersionNumber={"LatestVersion": True},
)
schema_definition = response["SchemaDefinition"]
schema = AvroSchema(schema_definition)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=serializer,
    compression_type="gzip"
)


def lambda_handler(event, context):

   for record in event['Records']:
      try:
         message_body = json.loads(record['body'])
         producer.send(TOPIC_NAME, value=(message_body,schema))
         producer.flush()
         logger.info(f"Message sent to Kafka topic {TOPIC_NAME}")

      except Exception as e:
         logger.error(f"Error processing message: {str(e)}")

   return {"statusCode": 200, "body": "Messages processed successfully"}

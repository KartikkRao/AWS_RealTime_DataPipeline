from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError

def lambda_handler(event, context):
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=["broker1_enpoint:9092","broker2_endpoint:9092"], 
            client_id='topic_creation'
            # Extra parameters if using access control check in documentation for the parameter
        )

        topic_list = [NewTopic(name="post_data", num_partitions=2, replication_factor=2)]  # can be configured anyhow you want go through kafka-python to know more paramters 

        
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        
        return {"Success": "Yes", "Message": "Topic 'post_data' created successfully."}

    except TopicAlreadyExistsError:
        return {"Success": "No", "Message": "Topic 'post_data' already exists. No action needed."}
    
    except KafkaError as e:
        return {"Success": "No", "Message": f"An error occurred while creating the topic: {e}"}

    except Exception as e:
        return {"Success": "No", "Message": f"Unexpected error: {e}"}

    finally:
        admin_client.close()

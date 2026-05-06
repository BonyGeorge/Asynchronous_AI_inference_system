import json
import boto3
from .data import load_test_data

def load_and_send_to_sqs(queue_name: str, data_path: str = "data/test_features.csv"):
    """Loads the test dataset and sends one message per record to SQS."""
    
    df = load_test_data(data_path)
    print(f"Loaded {len(df)} rows from test dataset.")
    
    sqs = boto3.client('sqs', region_name='us-east-1')
    
    try:
        response = sqs.get_queue_url(QueueName=queue_name)
        queue_url = response['QueueUrl']
    except sqs.exceptions.QueueDoesNotExist:
        raise ValueError(f"Queue '{queue_name}' not found. Please create it in AWS.")

    messages_sent = 0
    for index, row in df.iterrows():
        record_id = f"sample_{str(index).zfill(3)}"
        features = row.tolist()
            
        message_body = {
            "record_id": record_id,
            "features": features
        }
        
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message_body)
        )
        messages_sent += 1
        
    print(f"Successfully sent {messages_sent} messages to SQS!")
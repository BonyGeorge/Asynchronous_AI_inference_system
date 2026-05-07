import os
import time
import json
import boto3
import joblib
from datetime import datetime, timezone

QUEUE_NAME = os.getenv("QUEUE_NAME", "ml_pipeline_queue")
S3_BUCKET = os.getenv("S3_BUCKET", "async-ai-inf")
MODEL_KEY = os.getenv("MODEL_KEY", "models/20260505_230620/model.pkl") 

sqs = boto3.client('sqs', region_name='us-east-1')
s3 = boto3.client('s3', region_name='us-east-1')

def startup():
    """Requirement: Load the trained model from S3 on startup."""
    print(f"Starting consumer. Downloading model from s3://{S3_BUCKET}/{MODEL_KEY}...")
    os.makedirs("/tmp/models", exist_ok=True)
    local_model_path = "/tmp/models/model.pkl"
    
    s3.download_file(S3_BUCKET, MODEL_KEY, local_model_path)
    model = joblib.load(local_model_path)
    print("Model loaded successfully. Ready to process messages.")
    return model

def process_messages(model):
    """Polls SQS, performs inference, saves to S3, and deletes the message."""
    queue_url = sqs.get_queue_url(QueueName=QUEUE_NAME)['QueueUrl']
    
    print(f"Listening for messages on {QUEUE_NAME}...")
    
    while True:
        # 1. Poll SQS for messages (Long polling for 10 seconds)
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10
        )
        
        if 'Messages' not in response:
            print("No messages in queue. Waiting...")
            continue
            
        for message in response['Messages']:
            receipt_handle = message['ReceiptHandle']
            body = json.loads(message['Body'])
            
            record_id = body['record_id']
            features = body['features']
            
            print(f"Processing {record_id}...")
            
            # 2. Perform Inference
            prediction = model.predict([features])[0]
            
            # 3. Format Output Requirement
            result = {
                "record_id": record_id,
                "prediction": int(prediction),
                "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            }
            
            # 4. Write prediction to S3
            result_key = f"predictions/{record_id}.json"
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=result_key,
                Body=json.dumps(result, indent=4)
            )
            print(f"Saved prediction to s3://{S3_BUCKET}/{result_key}")
            
            # 5. Delete message ONLY after successful processing
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
            print(f"Deleted {record_id} from SQS.\n")

if __name__ == "__main__":
    loaded_model = startup()
    process_messages(loaded_model)
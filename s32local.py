import json
import boto3
import os
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize the S3 client
s3 = boto3.client('s3')

def lambda_handler(event, context):
    logger.info('Lambda function started')
    
    # Get the S3 bucket and object key from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    logger.info(f'Processing file: {key} from bucket: {bucket}')

    # Define the local file path in the /tmp directory
    local_file_path = f'/tmp/{os.path.basename(key)}'
    
    try:
        # Download the file from S3 to the /tmp directory
        s3.download_file(bucket, key, local_file_path)
        logger.info(f'File downloaded to {local_file_path}')

        # You can now process the file as needed
        # For example, read the contents of the file
        with open(local_file_path, 'r') as file:
            file_contents = file.read()
            logger.info(f'File contents: {file_contents}')

        # Optionally, you can perform further processing here
        
    except Exception as e:
        logger.error(f'Error downloading file: {e}')
        raise e

    return {
        'statusCode': 200,
        'body': json.dumps('File processed successfully')
    }

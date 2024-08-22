import json
import boto3
import time
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3 = boto3.client('s3')
textract = boto3.client('textract')
comprehend = boto3.client('comprehend')

def lambda_handler(event, context):
    logger.info('Lambda function started')
    
    try:
        # Get the S3 bucket and object key from the event
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        logger.info(f'Processing file: {key} from bucket: {bucket}')

        # Start Textract job to analyze the document
        response = textract.start_document_text_detection(
            DocumentLocation={
                'S3Object': {
                    'Bucket': bucket,
                    'Name': key
                }
            }
        )

        # Get the JobId from the response
        job_id = response['JobId']
        logger.info(f'Started Textract job with ID: {job_id}')

        # Poll for the job status
        while True:
            response = textract.get_document_text_detection(JobId=job_id)
            status = response['JobStatus']
            logger.info(f'Textract job status: {status}')
            
            if status in ['SUCCEEDED', 'FAILED']:
                break
            
            time.sleep(5)  # Wait before checking the job status again
        
        output = json.dumps(response)
        logger.debug(f'Textract response: {output}')

        # Check if the job succeeded
        if status == 'SUCCEEDED':
            logger.info('Textract job completed successfully')
            
            # Extract text and detect PII
            for item in response['Blocks']:
                if item['BlockType'] == 'LINE':
                    logger.debug(f'Processing line: {item["Text"]}')
                    # Detect PII entities using Comprehend
                    pii_response = comprehend.detect_pii_entities(Text=item["Text"], LanguageCode='en')
                    if pii_response["Entities"]:
                        logger.info(f'PII detected in line: {item["Text"]}')
                        output = output.replace(item["Text"], "*" * len(item["Text"]))
                        logger.debug(f'Redacted line in output')

            # Store the redacted document back to S3
            redacted_key = f'redacted/{key}'
            s3.put_object(Bucket=bucket, Key=redacted_key, Body=output.encode('utf-8'))
            logger.info(f'Redacted document saved to {redacted_key}')

            return {
                'statusCode': 200,
                'body': json.dumps(f'Redacted document saved to {redacted_key}')
            }
        else:
            logger.error('Textract job failed')
            return {
                'statusCode': 500,
                'body': json.dumps('Textract job failed')
            }
    except Exception as e:
        logger.error(f'An error occurred: {str(e)}', exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps(f'An error occurred: {str(e)}')
        }

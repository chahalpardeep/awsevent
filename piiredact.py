import json
import boto3
import time

# Initialize AWS clients
s3 = boto3.client('s3')
textract = boto3.client('textract')
comprehend = boto3.client('comprehend')

def lambda_handler(event, context):
    # Get the S3 bucket and object key from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

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
    print(f'Started job with ID: {job_id}')

    # Poll for the job status
    while True:
        response = textract.get_document_text_detection(JobId=job_id)
        status = response['JobStatus']
        print(f'Status: {status}')
        
        if status in ['SUCCEEDED', 'FAILED']:
            break
        
        time.sleep(5)  # Wait before checking the job status again

    # Check if the job succeeded
    if status == 'SUCCEEDED':
        # Extract text from the response
        document_text = ''
        for item in response['Blocks']:
            if item['BlockType'] == 'LINE':
                #document_text += item['Text'] + '\n'

                # Detect PII entities using Comprehend
                pii_response = comprehend.detect_pii_entities(Text=item["Text"], LanguageCode='en')
                if pii_response["Entities"]:
                    response.replace(item["Text"], "*" * len(item["Text"]))

        # Redact PII entities
        #redacted_text = document_text
        #for entity in pii_response['Entities']:
            #redacted_text = redacted_text.replace(entity['Text'], '*' * len(entity['Text']))

        # Store the redacted document back to S3
        redacted_key = f'redacted/{key}'
        s3.put_object(Bucket=bucket, Key=redacted_key, Body=response.encode('utf-8'))

        return {
            'statusCode': 200,
            'body': json.dumps(f'Redacted document saved to {redacted_key}')
        }
    else:
        return {
            'statusCode': 500,
            'body': json.dumps('Textract job failed')
        }
    

import boto3
import requests
import hashlib
import os
from botocore.exceptions import ClientError

# Constants for S3 bucket and file details
BUCKET_NAME = 'rearc-assessment'
S3_FILE_KEY = 'bls/pr.data.0.Current'  # Path of the file in the S3 bucket
# Base URL of the BLS website
BASE_URL = "https://download.bls.gov/pub/time.series/pr/"
WEBSITE_URL = BASE_URL + "pr.data.0.Current"
USER_AGENT = "MyLambdaApp/1.0 (contact@example.com)"  # Replace with a valid User-Agent

# S3 client
s3_client = boto3.client('s3')

def fetch_file_from_website(url):
    """
    Fetch the file content from the given URL using an HTTP GET request.
    """
    headers = {"User-Agent": USER_AGENT}
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise exception for HTTP errors
    print("Successfully fetched file from the website.")
    return response.text


def get_file_from_s3(bucket, key):
    """
    Retrieve the file content from S3. 
    If the file does not exist, return an empty string.
    """
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        print("Fetched existing file from S3.")
        return content
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print("File does not exist in S3. Treating as empty.")
            return ""  # File does not exist
        else:
            raise


def file_has_changed(existing_content, new_content):
    """
    Compare the hash of the existing content and the new content to detect changes.
    """
    existing_hash = hashlib.md5(existing_content.encode('utf-8')).hexdigest()
    new_hash = hashlib.md5(new_content.encode('utf-8')).hexdigest()

    print(f"Existing file hash: {existing_hash}")
    print(f"New file hash: {new_hash}")

    return existing_hash != new_hash


def upload_file_to_s3(content, bucket, key):
    """
    Upload the new file content to the specified S3 bucket.
    """
    s3_client.put_object(Bucket=bucket, Key=key, Body=content, ContentType='text/plain')
    print(f"File uploaded to S3: s3://{bucket}/{key}")

def sync_data():
    """
    AWS Lambda handler to fetch a file from a website, 
    compare it with the S3 file, and update S3 if needed.
    """
    try:
        # Fetch the file from the website
        new_file_content = fetch_file_from_website(WEBSITE_URL)

        # Check if the file exists in S3
        existing_file_content = get_file_from_s3(BUCKET_NAME, S3_FILE_KEY)

        # Compare the content using hash values
        if file_has_changed(existing_file_content, new_file_content):
            print("File content has changed. Updating S3...")
            upload_file_to_s3(new_file_content, BUCKET_NAME, S3_FILE_KEY)
            print("S3 file updated successfully.")
        else:
            print("No changes detected. S3 file is up-to-date.")

    except Exception as e:
        print(f"An error occurred: {e}")
        raise

if __name__ == "__main__":
    sync_data()
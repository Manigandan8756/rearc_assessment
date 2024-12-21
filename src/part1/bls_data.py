import boto3
import requests
import hashlib
import os
from botocore.exceptions import ClientError
from bs4 import BeautifulSoup
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from util.pipeline import *


def extract_file_names_from_website(url):
    """
    Wrangling the website data and return the file names
    from the HREF links
    """
    try:

        # Fetch the HTML content of the webpage
        user_agent = "MyApp/1.0 (contact@example.com)"
        response = trigger_get_request(url, user_agent)

        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.text, "html.parser")

        # Find all 'a' tags and extract their 'href' attributes
        href_links = [
            a["href"].split("/")[-1:][0] for a in soup.find_all("a", href=True)
        ]

        return href_links

    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL: {e}")
        return []
    except Exception as e:
        print(f"Error parsing HTML: {e}")
        return []


def fetch_file_from_website(url, file_name):
    """
    Fetch the file content from the given URL using an HTTP GET request.
    """
    # Added User-Agent to comply with BLS data policy
    user_agent = "MyApp/1.0 (contact@example.com)"
    response = trigger_get_request(url, user_agent)
    print(f"Successfully fetched file: {file_name} from the website.")
    return response.text


def get_file_from_s3(s3_client, bucket, key):
    """
    Retrieve the file content from S3.
    If the file does not exist, return an empty string.
    """
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("utf-8")
        print("Fetched existing file from S3.")
        return content
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            print("File does not exist in S3. Treating as empty.")
            return ""  # File does not exist
        else:
            raise


def file_has_changed(existing_content, new_content):
    """
    Compare the hash of the existing content and the new content to detect changes.
    """
    existing_hash = hashlib.md5(existing_content.encode("utf-8")).hexdigest()
    new_hash = hashlib.md5(new_content.encode("utf-8")).hexdigest()

    print(f"Existing file hash: {existing_hash}")
    print(f"New file hash: {new_hash}")

    return existing_hash != new_hash


def upload_file_to_s3(s3_client, content, bucket, key):
    """
    Upload the new file content to the specified S3 bucket.
    """
    write_data_to_s3(s3_client, bucket, key, content, "text/plain")
    # s3_client.put_object(Bucket=bucket, Key=key, Body=content, ContentType='text/plain')
    print(f"File uploaded to S3: s3://{bucket}/{key}")


def sync_data(bucket_name, base_url, s3_client):
    """
    Function to fetch a file from a website,
    compare it with the existing S3 file, and update to S3,
    if a change is identifed in the Source website file.
    """
    try:
        # Extract file names from the website
        file_names = extract_file_names_from_website(base_url)

        # Loop the file names to detect the changes and sync with S3 bucket
        for file_name in file_names:
            print(f"Started syncing the data of a file::{file_name}")
            website_url = base_url + file_name  # Complete URL of the BLS website
            s3_file_key = "bls/" + file_name  # Path of the file in the S3 bucket

            # Fetch the file from the website
            new_file_content = fetch_file_from_website(website_url, file_name)

            # Check if the file exists in S3
            existing_file_content = get_file_from_s3(s3_client, bucket_name, s3_file_key)

            # Compare the content using hash values
            if file_has_changed(existing_file_content, new_file_content):
                print("File content has changed. Updating S3...")
                upload_file_to_s3(s3_client, new_file_content, bucket_name, s3_file_key)
                print("S3 file updated successfully.")
            else:
                print("No changes detected. S3 file is up-to-date.")

    except Exception as e:
        print(f"An error occurred: {e}")
        raise


if __name__ == "__main__":
    # Constants for S3 bucket and file details
    bucket_name = "rearc-assessment"
    # Base URL of the BLS website
    base_url = (
        "https://download.bls.gov/pub/time.series/pr/"  
    )

    # S3 client
    s3_client = boto3.client("s3")
    sync_data(bucket_name, base_url, s3_client)

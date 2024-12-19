import requests
import boto3
import logging
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
import json
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class APIToS3Handler:
    """
    Class to read the USA population data from the Public server
    and upload it to the S3 bucket.
    """
    def __init__(self, api_url, s3_bucket, s3_key_prefix, aws_region="ap-south-1"):
        self.api_url = api_url
        self.s3_bucket = s3_bucket
        self.s3_key_prefix = s3_key_prefix
        self.s3_client = boto3.client("s3", region_name=aws_region)

    def fetch_data_from_api(self):
        """Fetch data from the given API URL."""
        logging.info("Fetching data from API: %s", self.api_url)
        try:
            response = requests.get(self.api_url, timeout=30)
            response.raise_for_status()  # Raise an HTTPError for bad responses
            logging.info("Data successfully fetched from API.")
            return response.json()["data"] # Parse the required content and return it
        except requests.exceptions.RequestException as e:
            logging.error("Error while fetching data from API: %s", e)
            raise

    def write_data_to_s3(self, data):
        """Write data to an S3 bucket."""
        s3_key = f"{self.s3_key_prefix}/usa_population_data.json" # File name to get stored
        
        logging.info("Writing data to S3 bucket: %s, Key: %s", self.s3_bucket, s3_key)
        try:
            # Serialize data to JSON
            serialized_data = json.dumps(data, indent=4)
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=serialized_data,
                ContentType="application/json"
            )
            logging.info("Data successfully written to S3: s3://%s/%s", self.s3_bucket, s3_key)
        except (NoCredentialsError, PartialCredentialsError) as cred_err:
            logging.error("AWS credentials error: %s", cred_err)
            raise
        except ClientError as client_err:
            logging.error("S3 client error: %s", client_err)
            raise
        except Exception as e:
            logging.error("An error occurred while writing data to S3: %s", e)
            raise

    def process(self):
        """Main process to fetch data from the API and write it to S3."""
        try:
            # Fetch data from the API
            data = self.fetch_data_from_api()
            
            # Write data to S3
            self.write_data_to_s3(data)
        except Exception as e:
            logging.error("Failed to process data from API to S3: %s", e)

if __name__ == "__main__":
    # Source API URL and S3 parameters
    API_URL = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
    S3_BUCKET = "rearc-assessment"
    S3_KEY_PREFIX = "usa_data"

    # Initialize and run the handler
    handler = APIToS3Handler(API_URL, S3_BUCKET, S3_KEY_PREFIX)
    handler.process()

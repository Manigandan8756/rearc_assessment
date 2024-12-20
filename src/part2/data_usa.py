import requests
import boto3
import logging
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
import json
from datetime import datetime
from util.pipeline import *
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
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
            response = trigger_get_request(self.api_url)
            logging.info("Data successfully fetched from API.")
            return response.json()["data"]  # Parse the required content and return it
        except requests.exceptions.RequestException as e:
            logging.error("Error while fetching data from API: %s", e)
            raise

    def write_data_to_s3(self, data):
        """Write data to an S3 bucket."""
        s3_key = (
            f"{self.s3_key_prefix}/usa_population_data.json"  # File name to get stored
        )

        logging.info("Writing data to S3 bucket: %s, Key: %s", self.s3_bucket, s3_key)
        serialized_data = json.dumps(data, indent=4)
        write_data_to_s3(
            self.s3_client, self.s3_bucket, s3_key, serialized_data, "application/json"
        )

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

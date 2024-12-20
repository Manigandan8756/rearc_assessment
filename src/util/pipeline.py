import requests
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
import logging


def trigger_get_request(url, user_agent="MyApp/1.0 (contact@example.com)"):
    """
    Fetch the file content from the given URL using an HTTP GET request.
    """
    headers = {"User-Agent": user_agent}
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise exception for HTTP errors for bad responses (4xx and 5xx)
    return response


def write_data_to_s3(s3_client, bucket, key, data, content_type):
    """Write data to an S3 bucket."""

    logging.info("Writing data to S3 bucket: %s, Key: %s", bucket, key)
    try:
        s3_client.put_object(
            Bucket=bucket, Key=key, Body=data, ContentType=content_type
        )
        logging.info("Data successfully written to S3: s3://%s/%s", bucket, key)
    except (NoCredentialsError, PartialCredentialsError) as cred_err:
        logging.error("AWS credentials error: %s", cred_err)
        raise
    except ClientError as client_err:
        logging.error("S3 client error: %s", client_err)
        raise
    except Exception as e:
        logging.error("An error occurred while writing data to S3: %s", e)
        raise

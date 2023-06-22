import logging
from json import dumps

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)

OBJECT_PREFIX = "history/spotify/"
OBJECT_NAME = OBJECT_PREFIX + "user{user_id}/user{user_id}_spotify_{timestamp}.json"


def upload_to_s3(data: str, bucket_name: str, object_name: str, s3_hook: S3Hook):
    """Upload JSON data to S3

    If data is a dict, it will be converted to a JSON string before uploading.

    Args:
        data (str): JSON string or dict to upload to S3
        bucket_name (str): Name of the S3 bucket to upload to
        object_name (str): Name of the object to upload
        s3_hook (S3Hook): S3Hook to connect to the S3 bucket
    """
    logger.info(f"Uploading to S3 bucket_name={bucket_name}, object_name={object_name}")
    logger.debug(f"data={data}")

    if isinstance(data, dict):
        data = dumps(data)
    s3_hook.load_string(
        string_data=data,
        bucket_name=bucket_name,
        key=object_name,
    )


def generate_object_name(user_id: int, timestamp: str) -> str:
    """Generate an object name for an S3 object

    Args:
        user_id (int): User ID
        timestamp (str): Timestamp in the format %Y-%m-%d_%H

    Returns:
        str: Object name
            :: history/spotify/user{user_id}/user{user_id}_spotify_{timestamp}.json
    """
    return OBJECT_NAME.format(user_id=user_id, timestamp=timestamp)

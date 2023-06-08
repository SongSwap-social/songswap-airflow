from json import dumps
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def upload_to_s3(data: str, bucket_name: str, object_name: str, s3_hook: S3Hook):
    """Upload JSON data to S3

    If data is a dict, it will be converted to a JSON string before uploading.

    Args:
        data (str): JSON string or dict to upload to S3
        bucket_name (str): Name of the S3 bucket to upload to
        object_name (str): Name of the object to upload
        s3_hook (S3Hook): S3Hook to connect to the S3 bucket
    """
    if isinstance(data, dict):
        data = dumps(data)
    s3_hook.load_string(
        string_data=data,
        bucket_name=bucket_name,
        key=object_name,
    )

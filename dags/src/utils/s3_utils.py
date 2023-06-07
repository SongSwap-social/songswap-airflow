from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def upload_to_s3(data, bucket_name, object_name, s3_hook: S3Hook):
    """Uploads a string to S3"""
    s3_hook.load_string(
        string_data=data,
        bucket_name=bucket_name,
        key=object_name,
    )

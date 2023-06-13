from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from src.utils.s3_utils import upload_to_s3

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 6, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


s3_hook = S3Hook(aws_conn_id="SongSwap_S3_TestPutGetDelete")


def verify_bucket_name_variable():
    """Verify that the bucket_name variable is set."""
    from airflow.models import Variable
    from airflow.exceptions import AirflowException

    bucket_name = Variable.get("bucket_name", default_var=None)
    if bucket_name is None:
        raise AirflowException("bucket_name variable is not set in Airflow")
    return "bucket_name", bucket_name


def upload_to_s3_test():
    """Upload a test object to S3."""
    upload_to_s3(
        data="Test data",
        bucket_name="songswap-history",
        object_name="test",
        s3_hook=s3_hook,
    )


def read_from_s3_test():
    """Read the test object from S3."""
    result = s3_hook.read_key(
        key="test",
        bucket_name="songswap-history",
    )
    assert result == "Test data"


def delete_from_s3_test():
    """Delete the test object from S3."""
    s3_hook.delete_objects(
        bucket="songswap-history",
        keys=["test"],
    )


with DAG(
    "test_s3",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    tags=["songswap", "sanity", "test"],
    description="Verify the variable 'bucket_name' is set, and test PUT, GET, and DELETE from S3",
    catchup=False,
) as dag:
    verify_bucket_name_variable = PythonOperator(
        task_id="verify_bucket_name_variable",
        python_callable=verify_bucket_name_variable,
    )

    upload_to_s3_test = PythonOperator(
        task_id="upload_to_s3_test",
        python_callable=upload_to_s3_test,
    )

    read_from_s3_test = PythonOperator(
        task_id="read_from_s3_test",
        python_callable=read_from_s3_test,
    )

    delete_from_s3_test = PythonOperator(
        task_id="delete_from_s3_test",
        python_callable=delete_from_s3_test,
    )

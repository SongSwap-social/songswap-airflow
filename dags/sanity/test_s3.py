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


def upload_to_s3_test():
    upload_to_s3(
        data="Test data",
        bucket_name="songswap-history",
        object_name="test",
        s3_hook=s3_hook,
    )


def read_from_s3_test():
    result = s3_hook.read_key(
        key="test",
        bucket_name="songswap-history",
    )
    assert result == "Test data"


def delete_from_s3_test():
    s3_hook.delete_objects(
        bucket="songswap-history",
        keys=["test"],
    )



dag = DAG(
    "test_s3",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    tags=["songswap", "sanity"],
    description="Test PUT, GET, and DELETE from S3",
    catchup=False,
)

t1 = PythonOperator(
    task_id="upload_to_s3_test",
    python_callable=upload_to_s3_test,
    dag=dag,
)

t2 = PythonOperator(
    task_id="read_from_s3_test",
    python_callable=read_from_s3_test,
    dag=dag,
)

t3 = PythonOperator(
    task_id="delete_from_s3_test",
    python_callable=delete_from_s3_test,
    dag=dag,
)

t1 >> t2 >> t3

from datetime import datetime
import json
import os
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from src.utils.s3_utils import upload_to_s3

logger = logging.getLogger(__name__)
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 6, 1),
}

# Load mock listening history data
filename_root = "user_spotify"
bucket_name = Variable.get("bucket_name")
expected_fail_message = "(AccessDenied) when calling the PutObject operation"


def load_test_data(filename: str):
    """Load mock listening history data"""
    test_data_filepath = os.path.join(os.path.dirname(__file__), "data", filename)
    with open(test_data_filepath, "r") as f:
        json_data = json.load(f)
    return json_data


def delete_from_s3(object_names: list, s3_delete_hook: S3Hook):
    """Delete mock listening history data from songswap-history S3 bucket

    Does not raise an error if the object does not exist.
    """
    try:
        s3_delete_hook.delete_objects(
            bucket=bucket_name,
            keys=object_names,
        )
        logger.info(f"Deleted {object_names} from {bucket_name}")
    except Exception as e:
        logger.error(f"Failed to delete {object_names} from {bucket_name}: {str(e)}")
        raise e


def test_upload_to_s3(
    json_data: str,
    object_name: str,
    s3_put_hook: S3Hook,
    should_fail: bool = False,
):
    """Upload mock listening history data to appropriate songswap-history S3 bucket"""
    try:
        upload_to_s3(
            data=json_data,
            bucket_name=bucket_name,
            object_name=object_name,
            s3_hook=s3_put_hook,
        )
        logger.info(f"Uploaded {object_name} to {bucket_name}")
        if should_fail:
            raise Exception(f"Upload should have failed: object_name={object_name}")
    except Exception as e:
        if should_fail:
            assert expected_fail_message in str(e), f"Unexpected error: {str(e)}"
            logger.info(f"Upload successfully failed: {str(e)}")
        else:
            raise e


with DAG(
    "test_s3_load_user_history",
    default_args=default_args,
    tags=["songswap", "unit", "test"],
    description="Test S3 user's permissions for loading listening history (PUT)",
    catchup=False,
    schedule_interval=None,
) as dag:
    # Format to 2023-06-01_00, YYYY-MM-DD_HH
    current_timestamp = datetime.now().strftime("%Y-%m-%d_%H")
    good_object_name = f"history/test/user/{filename_root}_{current_timestamp}.json"
    bad_object_name = f"test/user/test_{filename_root}_{current_timestamp}.json"
    bad_object_name2 = f"history/{filename_root}_{current_timestamp}.json"

    json_data = load_test_data(f"{filename_root}.json")

    s3_put_hook = S3Hook(aws_conn_id="SongSwap_S3_PutOnly")
    s3_delete_hook = S3Hook(aws_conn_id="SongSwap_S3_TestPutGetDelete")

    t1 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=test_upload_to_s3,
        op_args=[json_data, good_object_name, s3_put_hook],
    )

    # This will fail because the user should only have permission to write to the history/ prefix
    t2 = PythonOperator(
        task_id="upload_to_s3_unauthorized_prefix",
        python_callable=test_upload_to_s3,
        op_args=[json_data, bad_object_name, s3_put_hook, True],
    )

    # This will fail because the user should only have permission to write to the history/[apple|spotify]/user* prefix
    t3 = PythonOperator(
        task_id="upload_to_s3_unauthorized_object",
        python_callable=test_upload_to_s3,
        op_args=[json_data, bad_object_name2, s3_put_hook, True],
    )

    object_names = [good_object_name, bad_object_name, bad_object_name2]
    t4 = PythonOperator(
        task_id="delete_from_s3",
        python_callable=delete_from_s3,
        op_args=[object_names, s3_delete_hook],
        trigger_rule="all_done",  # Delete the objects even if one of the uploads failed
    )

    t1 >> t2 >> t3 >> t4

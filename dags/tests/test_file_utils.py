import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from src.utils.file_utils import (
    delete_file,
    read_json_file,
    save_json_file,
)

logger = logging.getLogger(__name__)

FILE_DIR = "/tmp"
FILE_NAME = "test.txt"
FILE_PATH = f"{FILE_DIR}/{FILE_NAME}"
JSON_DATA = {"name": "test"}


def _verify_json_data(json_data: dict, expected_json_data: dict):
    assert (
        json_data == expected_json_data
    ), f"JSON data does not match! {json_data} != {expected_json_data}"


def _verify_file_deleted(file_path: str):
    try:
        read_json_file(file_path)
        raise AssertionError(f"File still exists at {file_path}!")
    except FileNotFoundError:
        logger.info(f"File at {file_path} has been deleted.")
        pass


with DAG(
    "test_file_utils",
    start_date=datetime(2023, 6, 1),
    schedule_interval="@once",
    tags=["songswap", "test", "unit"],
    description="Tests for file_utils.py",
    catchup=False,
) as dag:
    save_file_task = PythonOperator(
        task_id="save_file",
        python_callable=save_json_file,
        op_kwargs={"json_data": JSON_DATA, "file_path": FILE_PATH},
    )

    read_file_task = PythonOperator(
        task_id="read_file",
        python_callable=read_json_file,
        op_kwargs={"file_path": FILE_PATH},
    )

    verify_json_data_task = PythonOperator(
        task_id="verify_json_data",
        python_callable=_verify_json_data,
        op_kwargs={"json_data": read_file_task.output, "expected_json_data": JSON_DATA},
    )

    delete_file_task = PythonOperator(
        task_id="delete_file",
        python_callable=delete_file,
        op_kwargs={"file_path": FILE_PATH},
    )

    verify_file_deleted_task = PythonOperator(
        task_id="verify_file_deleted",
        python_callable=_verify_file_deleted,
        op_kwargs={"file_path": FILE_PATH},
    )

    (
        save_file_task
        >> read_file_task
        >> verify_json_data_task
        >> delete_file_task
        >> verify_file_deleted_task
    )

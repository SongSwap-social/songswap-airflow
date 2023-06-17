import json
import logging
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from src.utils.discord_utils import discord_notification_on_failure
from src.utils.history_utils import (
    insert_history,
    transform_data,
    verify_inserted_history,
    verify_transformed_data_keys,
    verify_transformed_data_values,
)

logger = logging.getLogger(__name__)
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 6, 1),
}

# Create a PostgresHook instance
TEST_USER_ID = 0
TEST_USER_NAME = "testUser"
TEST_USER_EMAIL = "test@user.com"
TEST_USER_SPOTIFY_ID = "testSpotifyId"


def load_test_data(filename: str) -> dict:
    """Load mock listening history data

    Args:
        filename (str): The name of the file containing the mock listening history data.

    Returns:
        dict: The mock listening history data.
    """
    test_data_filepath = os.path.join(os.path.dirname(__file__), "data", filename)
    with open(test_data_filepath, "r") as f:
        json_data = json.load(f)
    return json_data


def verify_test_user_exists(postgres_hook: PostgresHook):
    """Verify that 'testUser' with id=0 exists in the database."""
    # Get the connection
    conn = postgres_hook.get_conn()
    # Create a cursor
    cursor = conn.cursor()
    # Execute a query
    cursor.execute('SELECT * FROM "Users" WHERE id=0;')
    # Fetch the result
    result = cursor.fetchone()
    # Verify the result: id, username, email, spotify_id
    assert result == (
        TEST_USER_ID,
        TEST_USER_NAME,
        TEST_USER_EMAIL,
        TEST_USER_SPOTIFY_ID,
    )


def test_transform_data(json_data: str, user_id: int) -> dict:
    """Test that the data is transformed correctly."""
    transformed_data = transform_data(json_data, user_id)
    logger.info(f"transformed_data: {transformed_data}")
    return transformed_data


def validate_transform_data_key_format(transformed_data: dict):
    """Validate that the keys in the transformed data are correct

    The keys should be the names of the tables in the database, and the values
    should be lists of dictionaries, where each dictionary is a row in the table.
    """
    verify_transformed_data_keys(transformed_data)


def validate_transform_data_value_format(transformed_data: dict):
    """Validate that the values in the transformed data are correct

    The values must be lists of dictionaries, where each dictionary is a row in
    the table. The values must not be empty.
    """
    verify_transformed_data_values(transformed_data)


def test_insert_history(transformed_data: dict, postgres_hook: PostgresHook):
    """Test that the data is inserted into the database correctly."""
    insert_history(transformed_data, postgres_hook)


def verify_data_inserted_correctly(transformed_data: dict, postgres_hook: PostgresHook):
    """Verify that the data was inserted correctly.

    Check the history table for the user, and verify that the data matches the
    data that was inserted.
    """
    verify_inserted_history(transformed_data, postgres_hook)


def cleanup_test_data(postgres_hook: PostgresHook, user_id: int):
    """Cleanup the inserted test data from the database."""

    def verify_test_data_deleted():
        """Verify that the test data was deleted from the database."""
        try:
            # Execute a query
            cursor.execute(
                f'SELECT 1 FROM "History" WHERE "user_id" = {user_id} LIMIT 1;'
            )
            # Fetch the result
            result = cursor.fetchone()
            # Verify the result
            assert result is None, f"Test data was not deleted: {result}"

        except Exception as e:
            logger.error(f"Failed to verify data was deleted: {str(e)}")
            raise e

    # Get the connection
    conn = postgres_hook.get_conn()
    # Create a cursor
    cursor = conn.cursor()

    try:
        # Execute DELETE statements
        cursor.execute(f'DELETE FROM "History" WHERE "user_id" = {user_id};')
        # Commit the changes
        conn.commit()
        # Verify that the test data was deleted
        verify_test_data_deleted()

    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to delete data: {str(e)}")
        raise e

    finally:
        cursor.close()
        conn.close()


with DAG(
    "test_rds_load_history",
    default_args=default_args,
    schedule_interval="@once",
    tags=["songswap", "test", "unit"],
    catchup=False,
    on_failure_callback=discord_notification_on_failure,
) as dag:
    filename_root = "user_spotify"
    json_data = load_test_data(f"{filename_root}.json")

    postgres_hook = PostgresHook(postgres_conn_id="SongSwap_RDS")

    t_verify_test_user_exists = PythonOperator(
        task_id="verify_test_user_exists",
        python_callable=verify_test_user_exists,
        op_kwargs={"postgres_hook": postgres_hook},
    )

    t_test_transform_data = PythonOperator(
        task_id="test_transform_data",
        python_callable=test_transform_data,
        op_kwargs={"json_data": json_data, "user_id": TEST_USER_ID},
    )

    # Validate the keys in the transformed data
    t_validate_transform_data_key_format = PythonOperator(
        task_id="validate_transform_data_key_format",
        python_callable=validate_transform_data_key_format,
        op_kwargs={
            "transformed_data": "{{ ti.xcom_pull(task_ids='test_transform_data') }}"
        },
    )

    # Validate that the values in the transformed data are correct
    t_validate_transform_data_value_format = PythonOperator(
        task_id="validate_transform_data_value_format",
        python_callable=validate_transform_data_value_format,
        op_kwargs={
            "transformed_data": "{{ ti.xcom_pull(task_ids='test_transform_data') }}"
        },
    )

    # Insert the data into the database
    t_test_insert_history = PythonOperator(
        task_id="test_insert_history",
        python_callable=test_insert_history,
        op_kwargs={
            "transformed_data": "{{ ti.xcom_pull(task_ids='test_transform_data') }}",
            "postgres_hook": postgres_hook,
        },
    )

    # Verify data was inserted correctly
    t_validate_inserted_history = PythonOperator(
        task_id="validate_inserted_history",
        python_callable=verify_data_inserted_correctly,
        op_kwargs={
            "transformed_data": "{{ ti.xcom_pull(task_ids='test_transform_data') }}",
            "postgres_hook": postgres_hook,
        },
    )

    # Try inserting again, verify no errors
    t_test_insert_history_again = PythonOperator(
        task_id="test_insert_history_again",
        python_callable=test_insert_history,
        op_kwargs={
            "transformed_data": "{{ ti.xcom_pull(task_ids='test_transform_data') }}",
            "postgres_hook": postgres_hook,
        },
    )

    # Delete the data that was inserted
    t_cleanup_test_data = PythonOperator(
        task_id="cleanup_test_data",
        python_callable=cleanup_test_data,
        op_kwargs={"postgres_hook": postgres_hook, "user_id": TEST_USER_ID},
    )

    (
        t_verify_test_user_exists
        >> t_test_transform_data
        >> [
            t_validate_transform_data_key_format,
            t_validate_transform_data_value_format,
        ]
        >> t_test_insert_history
        >> t_validate_inserted_history
        >> t_test_insert_history_again
        >> t_cleanup_test_data
    )

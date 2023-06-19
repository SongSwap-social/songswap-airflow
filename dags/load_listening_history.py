# dags/dags/load_listening_history.py

import logging
from datetime import datetime, timedelta
from json import dumps, loads

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from src.utils.discord_utils import discord_notification_on_failure

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _fetch_listening_history(
    user_id: str, tokens: dict, after_timestamp: int, pg_hook: PostgresHook
):
    """Fetches listening history from Spotify for a single user

    Handles refreshing the access token if it's expired.

    Args:
        user_id (str): The user's SongSwap ID
        tokens (dict): The user's Spotify access and refresh tokens
            :: {"access_token": str, "refresh_token": str}
        after_timestamp (int): Unix timestamp in milliseconds of the hour we want to fetch the listening history for.
        pg_hook (PostgresHook): PostgresHook to connect to the database
    """
    from spotipy.exceptions import SpotifyException
    from src.utils.spotify_utils import fetch_listening_history, refresh_access_token

    logger.info(f"Fetching Spotify listening history for user {user_id}")
    try:
        access_token = tokens["access_token"]
        # Fetch listening history for each user
        history = fetch_listening_history(
            access_token=access_token, after=after_timestamp
        )

    except SpotifyException as e:
        if e.http_status == 401:  # Unauthorized, access token was invalid/expired
            refresh_token = tokens["refresh_token"]
            # Fetch a fresh access_token using the refresh_token
            new_access_token = refresh_access_token(
                user_id=user_id, refresh_token=refresh_token, pg_hook=pg_hook
            )
            # Retry fetching the listening history with the new token
            history = fetch_listening_history(
                access_token=new_access_token, after=after_timestamp
            )
        else:
            raise e

    return history


def extract_spotify_history(after_timestamp: int = None) -> str:
    """Extracts listening history from Spotify for all users

    Returns a JSON string of the listening history for all users whose history is not empty.

    Args:
        after_timestamp (int): Unix timestamp in milliseconds of the hour we want to fetch the listening history for.
            The hour should be the previous hour rounded down to the nearest hour. e.g., at 10am we want to fetch the
            listening history for 9am-10am. Defaults to None, which will fetch the 50 most recent items in the user's
            listening history.

    Returns:
        str: JSON string of the listening history for all users whose history is not empty.
            :: {"user_id": {"items": [history_item, ...]}, ...}
        None: If there is no listening history to load
    """
    logger.info(
        f"Extracting Spotify listening history for hour after_timestamp={after_timestamp}"
    )
    from src.utils.spotify_utils import fetch_tokens

    # Fetch user_ids, tokens from database
    pg_hook = PostgresHook(postgres_conn_id="SongSwap_RDS")
    tokens = fetch_tokens(pg_hook)
    history = {}
    for user_id in tokens:
        try:
            # Fetch listening history for each user
            user_history = _fetch_listening_history(
                user_id=user_id,
                tokens=tokens[user_id],
                after_timestamp=after_timestamp,
                pg_hook=pg_hook,
            )
            # Check if listening history is empty
            if not user_history["items"]:
                logger.info(f"Listening history is empty for user {user_id}")
                continue

            history[user_id] = user_history
        except Exception as e:
            logger.error(f"Failed to process user {user_id}: {str(e)}")

    if not history:  # Short circuit if there's no history to load
        logger.info("No listening history to load")
        return False

    # Convert the history dictionary to a JSON string before returning it
    return dumps(history)


def load_spotify_history_to_s3(history: dict, after_timestamp: int = None):
    """Loads listening history from Spotify to S3 for all users whose history is not empty

    If the listening history for a user already exists in S3, it will not be overwritten.
    TODO: If the upload fails, and reaches the max retries, the history will be saved locally and the task will fail.

    Args:
        history (dict): JSON string of the listening history for a single user, as returned by `extract_spotify_history`
        after_timestamp (int): Unix timestamp in milliseconds of the hour we want to fetch the listening history for.
        context (dict): Context passed by Airfow
    """
    from airflow.models import Variable
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from src.utils.s3_utils import upload_to_s3

    timestamp = datetime.fromtimestamp(int(after_timestamp / 1000)).strftime(
        "%Y-%m-%d_%H"
    )
    logger.info(
        f"Loading Spotify listening history to S3 for after_timestamp={after_timestamp}, timestamp={timestamp}"
    )
    # Get bucket_name from Airflow Variable
    bucket_name = Variable.get("bucket_name")

    object_name = "history/spotify/user{user_id}/user{user_id}_spotify_{timestamp}.json"
    history = loads(history)  # Parse history from XCom string to dict
    logger.debug(f"history={history}")
    s3_hook = S3Hook(aws_conn_id="SongSwap_S3_PutOnly")
    # Upload all history data to S3
    for user_id in history:
        user_history = history[user_id]
        history_str = dumps(user_history)
        logger.info(f"user_id={user_id} len_items={len(user_history['items'])}")
        logger.debug(f"user_id={user_id}, history_str={history_str}")
        try:  # Try to upload the history to S3
            upload_to_s3(
                data=history_str,
                bucket_name=bucket_name,
                object_name=object_name.format(user_id=user_id, timestamp=timestamp),
                s3_hook=s3_hook,
            )
        except ValueError as e:
            logger.error(f"History for user {user_id} already exists: {str(e)}")


def load_spotify_history_to_rds(history: dict, after_timestamp: int = None):
    """Loads transformed listening history from Spotify of all users to RDS
    Args:
        history (dict): JSON string of the listening history for a single user, as returned by `extract_spotify_history`
        after_timestamp (int): Unix timestamp in milliseconds of the hour we want to fetch the listening history for.
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from src.utils.history_utils import transform_data
    from src.utils.rds_utils import insert_history_bulk

    timestamp = datetime.fromtimestamp(int(after_timestamp / 1000)).strftime(
        "%Y-%m-%d_%H"
    )
    logger.info(
        f"Loading Spotify listening to RDS history for after_timestamp={after_timestamp}, timestamp={timestamp}"
    )

    history = loads(history)  # Parse history from XCom string to dict
    logger.debug(f"history={history}")

    # Transform history to a format that can be inserted into RDS
    transformed_histories = []
    for user_id in history:
        user_history = history[user_id]
        transformed_histories.append(
            loads(transform_data(raw_data=user_history, user_id=user_id))
        )

    pg_hook = PostgresHook(postgres_conn_id="SongSwap_RDS")

    # Insert history into RDS
    insert_history_bulk(data_list=transformed_histories, pg_hook=pg_hook)


with DAG(
    dag_id="load_history",
    description="DAG to load listening histories from Spotify to S3 and RDS PostgreSQL",
    schedule_interval="1 * * * *",  # Runs hourly at minute 1
    start_date=datetime(2023, 6, 2),
    catchup=False,
    tags=["songswap"],
) as dag:
    # Timestamp of the hour we want to fetch the listening history for
    # The hour should be the previous hour rounded down to the nearest hour
    # Unix timestamp as an integer
    # e.g., at 10am we want to fetch the listening history for 9am-10am
    after_timestamp_int = int(
        (datetime.utcnow() - timedelta(hours=1))  # Previous hour
        .replace(minute=0, second=0, microsecond=0)  # Round down to the nearest hour
        .timestamp()  # Convert to Unix timestamp
        * 1000  # Convert to milliseconds
    )

    # Extract and load listening history for all users
    # Short circuit if there's no history to load, skip the load task
    extract_spotify_history_task = ShortCircuitOperator(
        task_id="extract_spotify_history",
        python_callable=extract_spotify_history,
        op_kwargs={"after_timestamp": after_timestamp_int},
        on_failure_callback=discord_notification_on_failure,
        retries=1,
    )

    load_spotify_history_to_s3_task = PythonOperator(
        task_id="load_spotify_history_to_s3",
        python_callable=load_spotify_history_to_s3,
        op_kwargs={
            "history": "{{ ti.xcom_pull(task_ids='extract_spotify_history') }}",
            "after_timestamp": after_timestamp_int,
        },
        on_failure_callback=discord_notification_on_failure,
    )

    load_spotify_history_to_rds_task = PythonOperator(
        task_id="load_spotify_history_to_rds",
        python_callable=load_spotify_history_to_rds,
        op_kwargs={
            "history": "{{ ti.xcom_pull(task_ids='extract_spotify_history') }}",
            "after_timestamp": after_timestamp_int,
        },
        on_failure_callback=discord_notification_on_failure,
    )

    (
        extract_spotify_history_task
        >> [load_spotify_history_to_s3_task, load_spotify_history_to_rds_task]
    )

# dags/load_listening_history.py

import logging
from datetime import datetime, timedelta
from json import dumps
from traceback import format_exc
from typing import List

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from src.utils.discord_utils import discord_notification_on_failure
from src.utils.file_utils import TMP_DIR, delete_file, save_json_file
from src.utils.s3_utils import upload_to_s3

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


def calculate_timestamp() -> int:
    """Calculates the after_timestamp for the previous hour rounded down to the nearest hour

    Return a unix timestamp for the hour that we want to fetch the listening history for.
    The hour should be the previous hour rounded down to the nearest hour.
    e.g., at 10am we want to fetch the listening history for 9am-10am

    Returns:
        int: Unix timestamp in milliseconds of the hour we want to fetch the listening history for
    """
    after_timestamp_int = int(
        (datetime.utcnow() - timedelta(hours=1))  # Previous hour
        .replace(minute=0, second=0, microsecond=0)  # Round down to the nearest hour
        .timestamp()  # Convert to Unix timestamp
        * 1000  # Convert to milliseconds
    )
    return after_timestamp_int


def _timestamp_to_string(after_timestamp: int) -> str:
    """Converts a Unix timestamp in milliseconds to a string in the format %Y-%m-%d_%H

    Args:
        after_timestamp (int): Unix timestamp in milliseconds of the hour we want to fetch the listening history for.
            ::  1627776000000

    Returns:
        str: String in the format %Y-%m-%d_%H
            ::  2021-08-01_00
    """
    string_timestamp = datetime.fromtimestamp(int(after_timestamp / 1000)).strftime(
        "%Y-%m-%d_%H"
    )
    logger.info(f"Converted timestamp {after_timestamp} to string {string_timestamp}")
    return string_timestamp


def extract_history(after_timestamp: int = None) -> dict:
    """Extracts listening history from Spotify for all users

    Returns a dict of the listening history for all users whose history is not empty.

    Args:
        after_timestamp (int): Unix timestamp in milliseconds of the hour we want to fetch the listening history for.
            The hour should be the previous hour rounded down to the nearest hour. e.g., at 10am we want to fetch the
            listening history for 9am-10am. Defaults to None, which will fetch the 50 most recent items in the user's
            listening history.

    Returns:
        dict: The listening history for all users whose history is not empty
            :: {"user_id": {"items": [history_item, ...]}, ...}
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
            logger.error(format_exc())

    if not history:  # Short circuit if there's no history to load
        logger.info("No listening history to load")
        return False

    return history


def load_history_to_s3(history: dict, after_timestamp: int = None):
    """Loads listening history from Spotify to S3 for all users whose history is not empty

    If the listening history for a user already exists in S3, it will not be overwritten.
    If the upload fails, the history data will be saved locally.
    # TODO, only save locally if the upload fails due to a network error (botocore.exceptions.ClientError)
        e.g. botocore.exceptions.ClientError: An error occurred (403) when calling the HeadObject operation: Forbidden
            This error occurs due to malformed headers, which is caused by a network error
    Args:
        history (dict): JSON string of the listening history for a single user, as returned by `extract_history`
        after_timestamp (int): Unix timestamp in milliseconds of the hour we want to fetch the listening history for.
    """
    from airflow.models import Variable
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from src.utils.s3_utils import generate_object_name, upload_to_s3

    timestamp = _timestamp_to_string(after_timestamp)
    logger.info(
        f"Loading Spotify listening history to S3 for after_timestamp={after_timestamp}, timestamp={timestamp}"
    )
    # Get bucket_name from Airflow Variable
    bucket_name = Variable.get("bucket_name")

    logger.debug(f"history={history}")
    s3_hook = S3Hook(aws_conn_id="SongSwap_S3_PutOnly")
    # Upload all history data to S3
    for user_id in history:
        user_history = history[user_id]
        user_object_name = generate_object_name(user_id, timestamp)
        logger.info(f"user_id={user_id} len_items={len(user_history['items'])}")
        try:  # Try to upload the history to S3
            history_str = dumps(user_history)
            logger.debug(f"user_id={user_id}, history_str={history_str}")
            upload_to_s3(
                data=history_str,
                bucket_name=bucket_name,
                object_name=user_object_name,
                s3_hook=s3_hook,
            )
        except ValueError as e:
            logger.error(f"History already exists for user {user_id}: {str(e)}")
        except Exception as e:
            logger.error(
                f"Failed to upload history for user {user_id}, saving JSON data locally: {str(e)}"
            )
            # Output a more verbose error message to give more insights into the error, include the traceback
            logger.error(format_exc())
            save_json_file(json_data=user_history, file_path=TMP_DIR + user_object_name)


def transform_history(history: dict) -> dict:
    """Transforms the listening history for all users whose history is not empty

    Args:
        history (dict): JSON string of the listening history for a single user, as returned by `extract_history`

    Returns:
        dict: The transformed listening history for all users whose history is not empty
            :: {"user_id": {"Artists": [], "Tracks": [], "ArtistTracks": [], "TrackPopularity": [], "History": []}, ...}
    """
    from src.utils.history_utils import transform_data

    def _flatten_transformed_histories(transformed_histories: List[dict]) -> dict:
        """Flattens a list of transformed histories into a single dict."""
        data = {
            "Artists": [],
            "Tracks": [],
            "ArtistTracks": [],
            "TrackPopularity": [],
            "History": [],
            "TrackImages": [],
            "TrackPreviews": [],
        }
        for transformed_history in transformed_histories:
            for table in data:
                data[table].extend(transformed_history[table])
        return data

    logger.info("Transforming Spotify listening history")
    logger.debug(f"history={history}")

    transformed_histories = []
    for user_id in history:
        user_history = history[user_id]
        transformed_history = transform_data(raw_data=user_history, user_id=user_id)
        transformed_histories.append(transformed_history)

    return _flatten_transformed_histories(transformed_histories)


def load_history_to_rds(transformed_histories: dict, after_timestamp: int = None):
    """Loads transformed listening history from Spotify of all users to RDS
    Args:
        transformed_histories (dict): The transformed listening history for all users whose history is not empty as returned by `transform_history`
        after_timestamp (int): Unix timestamp in milliseconds of the hour we want to fetch the listening history for.
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from src.utils.history_utils import insert_history_bulk

    timestamp = _timestamp_to_string(after_timestamp)
    logger.info(
        f"Loading Spotify listening to RDS history for after_timestamp={after_timestamp}, timestamp={timestamp}"
    )
    logger.debug(f"transformed_histories={transformed_histories}")
    # Insert history into RDS
    pg_hook = PostgresHook(postgres_conn_id="SongSwap_RDS")
    insert_history_bulk(transformed_data=transformed_histories, pg_hook=pg_hook)


def load_local_files_to_s3():
    """Loads all local files to S3"""
    from pathlib import Path

    from airflow.models import Variable
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    bucket_name = Variable.get("bucket_name")
    local_dir = Path(TMP_DIR) / "history/spotify"
    files = [
        (user_dir, user_json)
        for user_dir in local_dir.iterdir()
        for user_json in user_dir.iterdir()
    ]
    logger.info(f"Uploading {len(files)} local files to S3")
    s3_hook = S3Hook(aws_conn_id="SongSwap_S3_PutOnly")

    for user_dir, user_json in files:
        local_file_path = local_dir / user_dir / user_json
        data_str = local_file_path.read_text()
        object_name = str(local_file_path).replace(str(TMP_DIR), "", 1)
        try:
            logger.info(
                f"Attempting to upload to S3 bucket: local_file_path={local_file_path}"
            )
            upload_to_s3(data_str, bucket_name, object_name, s3_hook)
            logger.info(f"Successfully uploaded to S3. Deleting local file...")
            delete_file(str(local_file_path))
        except ValueError as e:
            logger.error(f"File already exists in S3: {str(e)}")
        except Exception as e:
            logger.error(f"Failed to upload local file {object_name} to S3: {str(e)}")
            logger.error(format_exc())


def extract_artist_data_from_history(history: dict) -> List[dict]:
    """Extracts artist data from the listening history of all users

    Args:
        history (dict): The listening history for all users whose history is not empty as returned by `extract_history`

    Returns:
        List[dict]: A list of artist data for all artists in the listening history of all users
    """
    from src.utils.artist_utils import (
        fetch_artists_data_with_dates,
        get_artists_and_dates_from_history,
        validate_assumption,
    )

    artists_and_dates = []
    for user_history in history.values():
        artists_and_dates.extend(get_artists_and_dates_from_history(user_history))

    artists_data = fetch_artists_data_with_dates(artists_and_dates)
    logger.info(f"Extracted {len(artists_data)} artists from Spotify listening history")
    validate_assumption(artists_data, artists_and_dates)
    return artists_data


def transform_artist_data(artists_data: List[dict]) -> dict:
    """Transforms artist data from Spotify into a format that can be loaded to RDS

    Args:
        artists_data (List[dict]): A list of artist data for all artists in the listening history of all users
            :: from `fetch_artists_data_with_dates`

    Returns:
        dict: A dict of transformed artist data for all artists in the listening history of all users
    """
    from src.utils.artist_utils import transform_data

    transformed_data = transform_data(artists_data)
    logger.info(f"Transformed {len(transformed_data)} artists from Spotify")
    return transformed_data


def load_artist_data_to_rds(transformed_data: dict):
    """Fetches metadata about artists in users' histories from Spotify and loads it to RDS

    Args:
        transformed_data (dict): A dict of transformed artist data for all artists in the listening history of all users
            :: from `transform_artist_data`

    Returns:
        dict: A dict of transformed artist data for all artists in the listening history of all users
    """
    from src.utils.artist_utils import insert_artist_bulk

    logger.info(f"Loading {len(transformed_data)} artists to RDS")
    pg_hook = PostgresHook(postgres_conn_id="SongSwap_RDS")
    insert_artist_bulk(transformed_data, pg_hook)


with DAG(
    dag_id="load_history",
    description="DAG to load listening histories from Spotify to S3 and RDS PostgreSQL",
    schedule_interval="1 * * * *",  # Runs hourly at minute 1
    start_date=datetime(2023, 6, 2),
    catchup=False,
    tags=["songswap"],
) as dag:
    calculate_timestamp_task = PythonOperator(
        task_id="calculate_timestamp",
        python_callable=calculate_timestamp,
        on_failure_callback=discord_notification_on_failure,
    )

    # Extract and load listening history for all users
    # Short circuit if there's no history to load, skip the load task
    extract_history_task = ShortCircuitOperator(
        task_id="extract_history",
        python_callable=extract_history,
        op_kwargs={"after_timestamp": calculate_timestamp_task.output},
        on_failure_callback=discord_notification_on_failure,
        retries=1,
    )

    load_history_to_s3_task = PythonOperator(
        task_id="load_history_to_s3",
        python_callable=load_history_to_s3,
        op_kwargs={
            "history": extract_history_task.output,
            "after_timestamp": calculate_timestamp_task.output,
        },
        on_failure_callback=discord_notification_on_failure,
    )
    load_local_files_to_s3_task = PythonOperator(
        task_id="load_local_files_to_s3",
        python_callable=load_local_files_to_s3,
        on_failure_callback=discord_notification_on_failure,
    )

    (extract_history_task >> load_history_to_s3_task >> load_local_files_to_s3_task)

    transform_history_task = PythonOperator(
        task_id="transform_history",
        python_callable=transform_history,
        op_kwargs={"history": extract_history_task.output},
        on_failure_callback=discord_notification_on_failure,
    )

    load_history_to_rds_task = PythonOperator(
        task_id="load_history_to_rds",
        python_callable=load_history_to_rds,
        op_kwargs={
            "transformed_histories": transform_history_task.output,
            "after_timestamp": calculate_timestamp_task.output,
        },
        on_failure_callback=discord_notification_on_failure,
    )

    extract_artist_data_from_history_task = PythonOperator(
        task_id="extract_artist_data_from_history",
        python_callable=extract_artist_data_from_history,
        op_kwargs={"history": extract_history_task.output},
        on_failure_callback=discord_notification_on_failure,
    )

    transform_artist_data_task = PythonOperator(
        task_id="transform_artist_data",
        python_callable=transform_artist_data,
        op_kwargs={
            "artists_data": extract_artist_data_from_history_task.output,
        },
        on_failure_callback=discord_notification_on_failure,
    )

    load_artist_data_to_rds_task = PythonOperator(
        task_id="load_artist_data_to_rds",
        python_callable=load_artist_data_to_rds,
        op_kwargs={
            "transformed_data": transform_artist_data_task.output,
        },
        on_failure_callback=discord_notification_on_failure,
    )

    (
        extract_history_task
        >> transform_history_task
        >> load_history_to_rds_task
        >> (
            extract_artist_data_from_history_task
            >> transform_artist_data_task
            >> load_artist_data_to_rds_task
        )
    )

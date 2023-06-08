# dags/dags/load_listening_history.py

import logging
from datetime import datetime, timedelta
from json import dumps, loads

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

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


def extract_spotify_history(after_timestamp: int = None):
    """Extracts listening history from Spotify for all users

    Returns a JSON string of the listening history for all users whose history is not empty.

    Args:
        after_timestamp (int): Unix timestamp in milliseconds of the hour we want to fetch the listening history for.
            The hour should be the previous hour rounded down to the nearest hour. e.g., at 10am we want to fetch the
            listening history for 9am-10am. Defaults to None, which will fetch the 50 most recent items in the user's
            listening history.
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

    # Convert the history dictionary to a JSON string before returning it
    return dumps(history)


def load_spotify_history(history: dict, after_timestamp: int = None):
    """Loads listening history from Spotify for a single user to S3

    Args:
        history (dict): JSON string of the listening history for a single user, as returned by `extract_spotify_history`
        after_timestamp (int): Unix timestamp in milliseconds of the hour we want to fetch the listening history for.
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from src.utils.s3_utils import upload_to_s3

    logger.info(
        f"Loading Spotify listening history for after_timestamp={after_timestamp}"
    )
    history = loads(history)  # Parse history from XCom string to dict
    logger.info(f"history={history}")
    s3_hook = S3Hook(aws_conn_id="SongSwap_S3_PutOnly")
    # Upload all history data to S3
    for user_id in history:
        user_history = history[user_id]
        history_str = dumps(user_history)
        logger.info(f"history_str={history_str}")
        timestamp = datetime.fromtimestamp(int(after_timestamp / 1000)).strftime(
            "%Y-%m-%d_%H"
        )
        bucket_name = f"songswap-history"
        prefix = f"history/spotify/user{user_id}"
        object_name = f"{prefix}/user{user_id}_spotify_{timestamp}.json"
        try:  # Try to upload the history to S3
            upload_to_s3(
                data=history_str,
                bucket_name=bucket_name,
                object_name=object_name,
                s3_hook=s3_hook,
            )
        except ValueError as e:
            logger.error(f"History for user {user_id} already exists: {str(e)}")


with DAG(
    dag_id="load_history",
    description="DAG to load listening histories from Spotify and Apple Music",
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
    extract_spotify_history_task = PythonOperator(
        task_id="extract_spotify_history",
        python_callable=extract_spotify_history,
        op_kwargs={"after_timestamp": after_timestamp_int},
    )

    load_spotify_history_task = PythonOperator(
        task_id="load_spotify_history",
        python_callable=load_spotify_history,
        op_kwargs={
            "history": "{{ ti.xcom_pull(task_ids='extract_spotify_history') }}",
            "after_timestamp": after_timestamp_int,
        },
    )

    extract_spotify_history_task >> load_spotify_history_task

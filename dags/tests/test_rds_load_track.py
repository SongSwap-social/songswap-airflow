import json
import logging
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from src.utils.discord_utils import discord_notification_on_failure
from src.utils.track_utils import fetch_tracks_data, get_track_ids_from_history

logger = logging.getLogger(__name__)
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 6, 1),
}


def load_test_data(filename: str) -> dict:
    """Load mock listening history data

    Args:
        filename (str): The name of the file containing the mock listening history data.

    Returns:
        dict: The mock listening history data.
    """
    test_data_filepath = os.path.join(os.path.dirname(__file__), "data", filename)
    with open(test_data_filepath, "r") as f:
        json_history = json.load(f)
    return json_history


def test_transform_data(
    json_history: dict, json_tracks: dict, fetched_tracks: dict
) -> dict:
    """Test that the data is transformed correctly."""
    # Create a `played_at` string in the format that the database expects
    # Date should be UTC now in format: 2023-06-17 03:59:08.851
    from src.utils.track_utils import transform_track_data

    played_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")

    logger.info("Testing transform_track_data with history")
    transformed_history = transform_track_data(json_history, played_at=played_at)
    logger.info("Testing transform_track_data with tracks")
    transformed_tracks = transform_track_data(json_tracks, played_at=played_at)
    logger.info("Testing transform_track_data with fetched_tracks")
    transformed_fetched_tracks = transform_track_data(
        fetched_tracks, played_at=played_at
    )

    return {
        "transformed_history": transformed_history,
        "transformed_tracks": transformed_tracks,
        "transformed_fetched_tracks": transformed_fetched_tracks,
    }


def validate_transform_data_key_format(transformed_data: dict):
    """Validate that the keys in the transformed data are correct

    The keys should be the names of the tables in the database, and the values
    should be lists of dictionaries, where each dictionary is a row in the table.
    """
    from src.utils.track_utils import validate_transformed_track_data_keys

    for key in transformed_data:
        logger.info(f"Verifying transformed data keys: {key}")
        data = transformed_data[key]
        validate_transformed_track_data_keys(data)


def validate_transform_data_value_format(transformed_data: dict):
    """Validate that the values in the transformed data are correct

    The values must be lists of dictionaries, where each dictionary is a row in
    the table. The values must not be empty.
    """
    from src.utils.track_utils import validate_transformed_track_data_values

    for key in transformed_data:
        logger.info(f"Verifying transformed data values: {key}")
        data = transformed_data[key]
        validate_transformed_track_data_values(data)


def validate_transformed_data_is_equal(transformed_data: dict) -> bool:
    """Validate that all transformed data (history, tracks, fetched) is equal."""
    import deepdiff

    # Get the transformed data
    transformed_history = transformed_data["transformed_history"]
    transformed_tracks = transformed_data["transformed_tracks"]
    transformed_fetched_tracks = transformed_data["transformed_fetched_tracks"]

    # Compare the transformed data
    # Ignore ['TrackPopularity']['popularity']
    # Ignore ['TrackPreviews']['url']
    kwargs = {
        "ignore_order": True,
        "exclude_regex_paths": [
            r"root\['TrackPopularity'\]\[\d+\]\['popularity'\]",
            r"root\['TrackPreviews'\]\[\d+\]\['url'\]",
        ],
    }
    diff_history_tracks = deepdiff.DeepDiff(
        transformed_history, transformed_tracks, **kwargs
    )

    diff_history_fetched_tracks = deepdiff.DeepDiff(
        transformed_history, transformed_fetched_tracks, **kwargs
    )
    diff_tracks_fetched_tracks = deepdiff.DeepDiff(
        transformed_tracks, transformed_fetched_tracks, **kwargs
    )

    # Verify that the transformed data is equal
    assert not diff_history_tracks, f"diff_history_tracks: {diff_history_tracks}"
    assert (
        not diff_history_fetched_tracks
    ), f"diff_history_fetched_tracks: {diff_history_fetched_tracks}"
    assert (
        not diff_tracks_fetched_tracks
    ), f"diff_tracks_fetched_tracks: {diff_tracks_fetched_tracks}"


with DAG(
    "test_rds_load_track",
    default_args=default_args,
    schedule_interval=None,
    tags=["songswap", "test", "unit"],
    catchup=False,
    on_failure_callback=discord_notification_on_failure,
    description="Test fetching, transforming and loading of track data to the database.\
        Transform data fetched from both the listening history and Spotify's /tracks endpoint, and verify they're the same.\
        NOTE: This DAG is not meant to be run on a schedule. ",
) as dag:
    filename_history = "user_spotify"
    json_history = load_test_data(f"{filename_history}.json")
    filename_tracks = "tracks_spotify"
    json_tracks = load_test_data(f"{filename_tracks}.json")

    postgres_hook = PostgresHook(postgres_conn_id="SongSwap_RDS")

    # Get the track IDs from the listening history data
    t_get_track_ids_from_history = PythonOperator(
        task_id="get_track_ids_from_history",
        python_callable=get_track_ids_from_history,
        op_kwargs={"history": json_history},
    )

    # Test that the data is fetched correctly
    t_fetch_tracks_data = PythonOperator(
        task_id="fetch_tracks_data",
        python_callable=fetch_tracks_data,
        op_kwargs={"track_ids": t_get_track_ids_from_history.output},
    )

    # Transform the data
    t_test_transform_data = PythonOperator(
        task_id="test_transform_data",
        python_callable=test_transform_data,
        op_kwargs={
            "json_history": json_history,
            "json_tracks": json_tracks,
            "fetched_tracks": t_fetch_tracks_data.output,
        },
    )

    # Validate the keys in the transformed data
    t_validate_transform_data_key_format = PythonOperator(
        task_id="validate_transform_data_key_format",
        python_callable=validate_transform_data_key_format,
        op_kwargs={"transformed_data": t_test_transform_data.output},
    )

    # Validate that the values in the transformed data are correct
    t_validate_transform_data_value_format = PythonOperator(
        task_id="validate_transform_data_value_format",
        python_callable=validate_transform_data_value_format,
        op_kwargs={"transformed_data": t_test_transform_data.output},
    )

    # Validate that the transformed data is equal
    t_validate_transformed_data_is_equal = PythonOperator(
        task_id="validate_transformed_data_is_equal",
        python_callable=validate_transformed_data_is_equal,
        op_kwargs={"transformed_data": t_test_transform_data.output},
    )

    (
        t_get_track_ids_from_history
        >> t_fetch_tracks_data
        >> t_test_transform_data
        >> [
            t_validate_transform_data_key_format,
            t_validate_transform_data_value_format,
            t_validate_transformed_data_is_equal,
        ]
    )

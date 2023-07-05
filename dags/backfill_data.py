"""The purpose of this DAG is to backfill the following data:

- Tracks: TrackImages, TrackPopularity, TrackPreviews
- Artists: ArtistImages, ArtistPopularity, ArtistFollowers, ArtistGenres
- TrackFeatures: TrackFeatures

Do not backfill data that has already been loaded into the database.

Backfill process:

1. Retrieve list of Track and Artist IDs that are not in the database tables above
    Tables include: TrackImages, TrackPopularity, TrackPreviews, ArtistImages, ArtistPopularity, ArtistFollowers, ArtistGenres, TrackFeatures
2. Batch the IDs into groups of 50 (for Tracks and Artists) and 100 (for TrackFeatures)
3. For each batch, make a request to the Spotify API to retrieve the data
4. Transform the data to fit the database schema
5. Load the data into the database
6. Validate the data was loaded correctly
"""

import logging
from datetime import datetime
from typing import List, Tuple

import src.utils.artist_utils as artist_utils
import src.utils.track_utils as track_utils
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extensions import cursor
from src.utils.rds_utils import fetch_query_results_in_chunks, get_database_cursor

logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 7, 3),
}


def _create_date() -> str:
    """Create a date string in the format '2023-07-03 16:21:55.532'

    Returns:
        str: A date string
    """
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")


def _add_date_to_artist_ids(artist_ids: List[str]) -> List[Tuple[str, str]]:
    """Add the current UTC date to a list of artist IDs
        Date is formatted as "2023-07-03 16:21:55.532

    Args:
        artist_ids (List[str]): A list of artist IDs

    Returns:
        List[Tuple[str, str]]: A list of tuples containing the artist ID and the current date
    """
    date = _create_date()
    return [(artist_id, date) for artist_id in artist_ids]


def _flatten_list_of_tuples(list_of_tuples: List[Tuple]) -> List[str]:
    """Flatten a list of tuples to a list

    Args:
        list_of_tuples (List[Tuple]): A list/set of tuples
            :: (('7vlddLsCrx8WdhCHgvHd7b',), ('6Wlk7S1iDbCGE6wAdp8gt2',), ('2Y67qsABsPKMrvCxPCzL6r',),

    Returns:
        List[str]: A list of strings
            :: ['7vlddLsCrx8WdhCHgvHd7b', '6Wlk7S1iDbCGE6wAdp8gt2', '2Y67qsABsPKMrvCxPCzL6r']
    """
    return [item for sublist in list_of_tuples for item in sublist]


def get_track_ids(cursor: cursor) -> List[str]:
    """Retrieve track IDs that are not in the database tables

    Returns:
        List[str]: A list of track IDs with no duplicates
    """
    query = """
    SELECT t.id
    FROM "Tracks" t
    WHERE t.id NOT IN (
        SELECT DISTINCT id
        FROM "TrackPreviews"
    )
    """
    track_ids = fetch_query_results_in_chunks(cursor, query)
    # Convert the set of tuples to a set of strings
    track_ids = list(set(_flatten_list_of_tuples(track_ids)))
    logger.info(
        f"Retrieved {len(track_ids)} track IDs that are not in the TrackPreviews table"
    )
    return track_ids


def get_track_features_ids(cursor: cursor) -> List[str]:
    """Retrieve track IDs that are not in the database tables

    Returns:
        List[str]: A list of track IDs with no duplicates
    """
    query = """
    SELECT t.id
    FROM "Tracks" t
    WHERE t.id NOT IN (
        SELECT DISTINCT id
        FROM "TrackFeatures"
    )
    """
    track_ids = fetch_query_results_in_chunks(cursor, query)
    # Convert the set of tuples to a set of strings
    track_ids = list(set(_flatten_list_of_tuples(track_ids)))
    logger.info(
        f"Retrieved {len(track_ids)} track IDs that are not in the TrackFeatures table"
    )
    return track_ids


def get_artist_ids(cursor: cursor) -> List[str]:
    """Retrieve artist IDs that are not in the database tables

    Returns:
        List[str]: A list of artist IDs with no duplicates
    """
    query = """
    SELECT a.id
    FROM "Artists" a
    WHERE a.id NOT IN (
        SELECT DISTINCT id
        FROM "ArtistImages"
    )
    """
    artist_ids = fetch_query_results_in_chunks(cursor, query)
    # Convert the set of tuples to a set of strings
    artist_ids = list(set(_flatten_list_of_tuples(artist_ids)))
    logger.info(
        f"Retrieved {len(artist_ids)} artist IDs that are not in the ArtistImages table"
    )

    return _add_date_to_artist_ids(artist_ids)


with DAG(
    dag_id="backfill_data",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["backfill", "songswap"],
) as dag:
    pg_hook = PostgresHook(postgres_conn_id="SongSwap_RDS")
    conn, cursor = get_database_cursor(pg_hook)

    # Retrieve artist IDs that are not in the ArtistImages table
    get_artist_ids_task = PythonOperator(
        task_id="get_artist_ids",
        python_callable=get_artist_ids,
        op_kwargs={"cursor": cursor},
    )

    # Retrieve artist data for multiple artists from the Spotify API
    fetch_artists_data_task = PythonOperator(
        task_id="fetch_artists_data",
        python_callable=artist_utils.fetch_artists_data_with_dates,
        op_kwargs={"artists_and_dates": get_artist_ids_task.output},
    )

    # Transform artist data to fit the database schema
    transform_artist_data_task = PythonOperator(
        task_id="transform_artist_data",
        python_callable=artist_utils.transform_data,
        op_kwargs={"raw_data": fetch_artists_data_task.output},
    )

    # Load artist data into the database
    # load_artist_data_task = PythonOperator(
    #     task_id="load_artist_data",
    #     python_callable=artist_utils.insert_artist_bulk,
    #     op_kwargs={
    #         "transformed_data": transform_artist_data_task.output,
    #         "pg_hook": pg_hook,
    #     },
    # )

    (
        get_artist_ids_task
        >> fetch_artists_data_task
        >> transform_artist_data_task
        # >> load_artist_data_task
    )

    # Retrieve track IDs that are not in the TrackPreviews table
    get_track_ids_task = PythonOperator(
        task_id="get_track_ids",
        python_callable=get_track_ids,
        op_kwargs={"cursor": cursor},
    )

    # Retrieve track data for multiple tracks from the Spotify API
    fetch_tracks_data_task = PythonOperator(
        task_id="fetch_tracks_data",
        python_callable=track_utils.fetch_tracks_data,
        op_kwargs={"track_ids": get_track_ids_task.output},
    )

    # Transform track data to fit the database schema
    transform_track_data_task = PythonOperator(
        task_id="transform_track_data",
        python_callable=track_utils.transform_track_data,
        op_kwargs={
            "raw_track_data": fetch_tracks_data_task.output,
            "played_at": _create_date(),
        },
    )

    # Load track data into the database
    # load_track_data_task = PythonOperator(
    #     task_id="load_track_data",
    #     python_callable=track_utils.insert_track_bulk,
    #     op_kwargs={
    #         "transformed_data": transform_track_data_task.output,
    #         "pg_hook": pg_hook,
    #     },
    # )

    (
        get_track_ids_task
        >> fetch_tracks_data_task
        >> transform_track_data_task
        # >> load_track_data_task
    )

    # Retrieve track IDs that are not in the TrackFeatures table
    get_track_features_ids_task = PythonOperator(
        task_id="get_track_features_ids",
        python_callable=get_track_features_ids,
        op_kwargs={"cursor": cursor},
    )

    # Retrieve track features data for multiple tracks from the Spotify API
    fetch_track_features_data_task = PythonOperator(
        task_id="fetch_track_features_data",
        python_callable=track_utils.fetch_track_features_data,
        op_kwargs={"track_ids": get_track_features_ids_task.output},
    )

    # Transform track features data to fit the database schema
    transform_track_features_data_task = PythonOperator(
        task_id="transform_track_features_data",
        python_callable=track_utils.transform_track_features_data,
        op_kwargs={"raw_track_features": fetch_track_features_data_task.output},
    )

    # Load track features data into the database
    # load_track_features_data_task = PythonOperator(
    #     task_id="load_track_features_data",
    #     python_callable=track_utils.insert_track_features_bulk,
    #     op_kwargs={
    #         "transformed_data": transform_track_features_data_task.output,
    #         "pg_hook": pg_hook,
    #     },
    # )

    (
        get_track_features_ids_task
        >> fetch_track_features_data_task
        >> transform_track_features_data_task
        # >> load_track_features_data_task
    )

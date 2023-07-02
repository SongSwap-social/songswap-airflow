"""Utility functions for transforming and loading listening history data to RDS."""
import logging
from json import dumps, loads
from typing import List, Set, Tuple, Union

from airflow.providers.postgres.hooks.postgres import PostgresHook
from dateutil.parser import parse
from src.utils.rds_utils import (
    fetch_query_results_in_chunks,
    get_database_cursor,
    insert_bulk,
)

logger = logging.getLogger(__name__)


def transform_data(raw_data: List[dict], user_id: int) -> dict:
    """
    Transforms raw JSON Spotify data into a format compatible with insert_history function.

    Args:
        raw_data (list): List of raw JSON Spotify data.
        user_id (int): User ID of the user whose data is being transformed.

    Returns:
        dict: Transformed data containing artists, tracks, artist_tracks and history.
    """

    # Initialize data
    data = {
        "Artists": [],
        "Tracks": [],
        "ArtistTracks": [],
        "TrackPopularity": [],
        "History": [],
        "TrackImages": [],
        "TrackPreviews": [],
    }

    # Transform raw_data
    for track_played in raw_data["items"]:
        track_played: dict
        track: dict = track_played.get("track")

        if not track:
            continue

        artists = track.get("artists")
        album = track.get("album")
        images = album.get("images")
        track_id: str = track.get("id")
        track_name: str = track.get("name")
        track_duration_ms = track.get("duration_ms")
        track_popularity = track.get("popularity")
        preview_url = track.get("preview_url")
        played_at = track_played.get("played_at")

        for idx, artist in enumerate(artists):
            artist: dict
            artist_id = artist.get("id")
            artist_name = artist.get("name")

            data["Artists"].append(
                {
                    "id": artist_id,
                    "name": artist_name,
                }
            )

            data["ArtistTracks"].append(
                {
                    "artist_id": artist_id,
                    "track_id": track_id,
                    "is_primary": idx == 0,
                }
            )

        data["Tracks"].append(
            {
                "id": track_id,
                "name": track_name,
                "duration_ms": track_duration_ms,
            }
        )

        data["TrackPopularity"].append(
            {
                "id": track_id,
                "date": played_at,
                "popularity": track_popularity,
            }
        )

        for image in images:
            image: dict
            height = image.get("height")
            width = image.get("width")
            url = image.get("url")

            data["TrackImages"].append(
                {
                    "id": track_id,
                    "height": height,
                    "width": width,
                    "url": url,
                }
            )

        data["TrackPreviews"].append(
            {
                "id": track_id,
                "url": preview_url,
            }
        )

        data["History"].append(
            {
                "user_id": user_id,  # Composite primary key
                "track_id": track_id,
                "played_at": played_at,  # Composite primary key
            }
        )

    return data


def verify_transformed_data_keys(data: dict):
    """Verify that the transformed data contains the correct keys.

    The keys should be the names of the tables in the database, and the values
    should be lists of dictionaries, where each dictionary is a row in the table.

    Args:
        data (dict): The transformed data to be verified.
    """
    data = parse_data(data)

    logger.info(f"Verifying transformed data keys: {data}")

    assert len(data) == 7, f"Transformed data should contain 5 tables: {data.keys()}"
    expected_table_keys = [  # Expected table names
        "Artists",
        "Tracks",
        "ArtistTracks",
        "TrackPopularity",
        "History",
        "TrackImages",
        "TrackPreviews",
    ]

    # Verify that the expected table keys are present
    assert set(data.keys()) == set(expected_table_keys), (
        f"Transformed data should contain the following tables: "
        f"{expected_table_keys}, but instead contains {data.keys()}"
    )

    expected_artist_keys = ["id", "name"]
    assert set(data["Artists"][0].keys()) == set(expected_artist_keys), (
        f"Table 'Artists' should contain the following columns: "
        f"{expected_artist_keys}, but instead contains "
        f"{data['Artists'][0].keys()}"
    )

    expected_artist_track_keys = ["artist_id", "track_id", "is_primary"]
    assert set(data["ArtistTracks"][0].keys()) == set(expected_artist_track_keys), (
        f"Table 'ArtistTracks' should contain the following columns: "
        f"{expected_artist_track_keys}, but instead contains "
        f"{data['ArtistTracks'][0].keys()}"
    )

    expected_track_keys = ["id", "name", "duration_ms"]
    assert set(data["Tracks"][0].keys()) == set(expected_track_keys), (
        f"Table 'Tracks' should contain the following columns: "
        f"{expected_track_keys}, but instead contains "
        f"{data['Tracks'][0].keys()}"
    )

    expected_track_popularity_keys = ["id", "date", "popularity"]
    assert set(data["TrackPopularity"][0].keys()) == set(
        expected_track_popularity_keys,
    ), (
        f"Table 'TrackPopularity' should contain the following columns: "
        f"{expected_track_popularity_keys}, but instead contains "
        f"{data['TrackPopularity'][0].keys()}"
    )

    expected_track_image_keys = ["id", "height", "width", "url"]
    assert set(data["TrackImages"][0].keys()) == set(expected_track_image_keys), (
        f"Table 'TrackImages' should contain the following columns: "
        f"{expected_track_image_keys}, but instead contains "
        f"{data['TrackImages'][0].keys()}"
    )

    expected_track_preview_keys = ["id", "url"]
    assert set(data["TrackPreviews"][0].keys()) == set(expected_track_preview_keys), (
        f"Table 'TrackPreviews' should contain the following columns: "
        f"{expected_track_preview_keys}, but instead contains "
        f"{data['TrackPreviews'][0].keys()}"
    )

    expected_history_keys = ["user_id", "track_id", "played_at"]
    assert set(data["History"][0].keys()) == set(expected_history_keys), (
        f"Table 'History' should contain the following columns: "
        f"{expected_history_keys}, but instead contains "
        f"{data['History'][0].keys()}"
    )


def verify_transformed_data_values(data: dict):
    """Verify that the transformed data contains the correct values.

    Args:
        data (dict): The transformed data to be verified.
    """
    data = parse_data(data)

    logger.info(f"Verifying transformed data values: {data}")

    for table_name, table in data.items():
        # Verify that the values are not empty
        assert len(table) > 0, f"Table {table_name} is empty"

        for row in table:
            # Verify that the values are all dictionaries
            assert isinstance(
                row, dict
            ), f"Table {table_name} contains non-dictionary values: {row}"

            # Verify that the values are not null
            for field, value in row.items():
                assert (
                    value is not None
                ), f"Table {table_name}, Row {row} contains null values at field: {field}"


def parse_data(data: Union[dict, str]) -> dict:
    """Parse data from JSON string to dictionary.

    Args:
        data (Union[dict, str]): The data to be parsed.
            e.g. :: {"History": [{"user_id": 1, "track_id": 1, "played_at": "2021-01-01 00:00:00"}]}

                Returns:
        dict: The parsed data.

    """
    if isinstance(data, str):
        data = loads(data)
    return data


def insert_history_bulk(transformed_data: dict, pg_hook: PostgresHook):
    """Insert history for multiple users into the database

    Args:
        transformed_data (dict): The transformed data to be inserted. `data` should be
            dictionary with keys: "Artists", "Tracks", "ArtistsTracks", "TrackImages", "History", etc.
        pg_hook (PostgresHook): Hook to connect to the database.
    """
    transformed_data = parse_data(transformed_data)
    insert_bulk(transformed_data, pg_hook)


def _prepare_history_data_for_comparison(data: dict) -> Set[Tuple]:
    """Prepare history data for comparison - `data` should be a dictionary with keys
    "Artists", "Tracks", "ArtistsTracks", "TrackImages", "History", etc., each of which
    should map to a list of dictionaries.

    `data` is from the `transform_data` function above.

    Args:
        data (dict): The data to be prepared.

    Returns:
        Set[Tuple]: A set of tuples containing the user_id, track_id, and played_at
            values from the history data.
    """
    history_data = set(
        (
            item["user_id"],
            item["track_id"],
            parse(item["played_at"]).replace(tzinfo=None),
        )
        for item in data["History"]
    )
    return history_data


def _construct_where_clause(data: dict) -> str:
    """Construct a WHERE clause for the select query.

    Args:
        data (dict): The data to be used to construct the WHERE clause.
            e.g. :: {"History": [{"user_id": 1, "track_id": 1, "played_at": "2021-01-01 00:00:00"}]}

    Returns:
        str: The WHERE clause.
            e.g. :: "user_id IN (1, 2) AND played_at IN ('2021-01-01 00:00:00', '2021-01-02 00:00:00')"
    """
    # ? Convert each value to a string and join them with commas ? Not sure if mapping is necessary
    user_ids = ", ".join(map(str, set(item["user_id"] for item in data["History"])))
    played_at_values = ", ".join(f"'{item['played_at']}'" for item in data["History"])
    where_in_data = f"user_id IN ({user_ids}) AND played_at IN ({played_at_values})"
    return where_in_data


def _construct_select_query(table: str, where_clause: str) -> str:
    """Construct a SELECT query.

    Args:
        table (str): The name of the table to select from.
        where_clause (str): The WHERE clause to be used in the query.

    Returns:
        str: The SELECT query.
            e.g. ::
                "SELECT "user_id", "track_id", "played_at"
                FROM "History"
                WHERE user_id IN (1, 2) AND played_at IN ('2021-01-01 00:00:00', '2021-01-02 00:00:00')"
    """
    query = (
        f'SELECT "user_id", "track_id", "played_at" FROM "{table}" WHERE {where_clause}'
    )
    return query


def _assert_matching_data(data1: Set[Tuple], data2: Set[Tuple]):
    """Assert that the transformed data and queried data match."""
    assert data1 == data2, (
        "Transformed data and queried data do not match: " f"{data1} != {data2}"
    )


# TODO Should just pass the `pg_cursor` instead of the `pg_hook`
def verify_inserted_history(transformed_data: dict, pg_hook: PostgresHook):
    """Verify that the transformed data has been inserted into the database."""
    transformed_data = parse_data(transformed_data)
    conn, cursor = get_database_cursor(pg_hook)

    history_data = _prepare_history_data_for_comparison(transformed_data)
    where_clause = _construct_where_clause(transformed_data)
    query = _construct_select_query("History", where_clause)

    try:
        queried_history = fetch_query_results_in_chunks(cursor, query)
        _assert_matching_data(history_data, queried_history)

    except Exception as e:
        logger.error(f"Failed to query data: {str(e)}")
        raise e

    finally:
        cursor.close()
        conn.close()

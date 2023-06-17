"""Utility functions for transforming and loading listening history data to RDS."""
import logging
from json import dumps, loads
from typing import List

from airflow.providers.postgres.hooks.postgres import PostgresHook
from dateutil.parser import parse

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
    }

    # Transform raw_data
    for track_played in raw_data["items"]:
        track_played: dict
        track: dict = track_played.get("track")

        if not track:
            continue

        artists = track.get("artists")
        track_id: str = track.get("id")
        track_name: str = track.get("name")
        track_duration_ms = track.get("duration_ms")
        track_popularity = track.get("popularity")
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
                "track_id": track_id,
                "date": played_at,
                "popularity": track_popularity,
            }
        )

        data["History"].append(
            {
                "user_id": user_id,  # Composite primary key
                "track_id": track_id,
                "played_at": played_at,  # Composite primary key
            }
        )

    # return data as JSON string
    return dumps(data)


def verify_transformed_data_keys(data: dict):
    """Verify that the transformed data contains the correct keys.

    The keys should be the names of the tables in the database, and the values
    should be lists of dictionaries, where each dictionary is a row in the table.

    Args:
        data (dict): The transformed data to be verified.
    """
    # Try to convert str to dict
    if isinstance(data, str):
        data = loads(data)

    logger.info(f"Verifying transformed data keys: {data}")

    assert len(data) == 5, f"Transformed data should contain 5 tables: {data.keys()}"
    expected_table_keys = [  # Expected table names
        "Artists",
        "Tracks",
        "ArtistTracks",
        "TrackPopularity",
        "History",
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

    expected_track_popularity_keys = ["track_id", "date", "popularity"]
    assert set(data["TrackPopularity"][0].keys()) == set(
        expected_track_popularity_keys,
    ), (
        f"Table 'TrackPopularity' should contain the following columns: "
        f"{expected_track_popularity_keys}, but instead contains "
        f"{data['TrackPopularity'][0].keys()}"
    )

    expected_history_keys = ["user_id", "track_id", "played_at"]
    assert set(data["History"][0].keys()) == set(expected_history_keys), (
        f"Table 'History' should contain the following columns: "
        f"{expected_history_keys}, but instead contains "
        f"{data['History'][0].keys()}"
    )


# TODO: HORRIBLY inefficient. Fix this. ORM is nice, but the decoupling is not worth it yet.
def verify_transformed_data_values(data: dict):
    """Verify that the transformed data contains the correct values.

    Args:
        data (dict): The transformed data to be verified.
    """
    if isinstance(data, str):
        data = loads(data)

    logger.info(f"Verifying transformed data values: {data}")
    # Verify that the values are not empty
    for table in data.values():
        assert len(table) > 0, f"Table {table} is empty"

    # Verify that the values are all dictionaries
    for table in data.values():
        for row in table:
            assert isinstance(
                row, dict
            ), f"Table {table} contains non-dictionary values: {row}"

    # Verify that the values are not null
    for table in data.values():
        for row in table:
            for value in row.values():
                assert value is not None, f"Table {table} contains null values: {row}"


def insert_history(data: dict, pg_hook: PostgresHook):
    """Insert history data into the database.

    Args:
        data (dict): The data to be inserted. It should be a dictionary with keys
            "artists", "tracks", "artist_tracks" and "history", each of which
            should map to a list of dictionaries.
        pg_hook (PostgresHook): Hook to connect to the database.
    """

    if isinstance(data, str):
        data = loads(data)

    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Define a helper function for bulk inserting
    def bulk_insert(table: str, data: List[dict]):
        """Bulk insert data into a table.

        Args:
            table (str): The name of the table to insert into.
            data (List[dict]): The data to be inserted. It should be a list of dictionaries.
        """
        from psycopg2.extras import execute_values

        if data:
            columns = data[0].keys()
            query = f"INSERT INTO \"{table}\" ({', '.join(columns)}) VALUES %s ON CONFLICT DO NOTHING"
            logger.info(f"Query: {query}")

            # Convert the dictionaries to tuples
            tuple_data = [tuple(item.values()) for item in data]
            logger.info(f"Inserting data into table {table}: {tuple_data}")

            execute_values(cursor, query, tuple_data)

    # Insert into each table
    try:
        bulk_insert("Artists", data.get("Artists"))
        bulk_insert("Tracks", data.get("Tracks"))
        bulk_insert("ArtistTracks", data.get("ArtistTracks"))
        bulk_insert("History", data.get("History"))
        conn.commit()

    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to insert data: {str(e)}")
        raise e

    finally:
        cursor.close()
        conn.close()


def verify_inserted_history(transformed_data: dict, pg_hook: PostgresHook):
    """Verify that RDS contains the transformed data.

    This function will query the database and verify that the data matches the
    transformed data. It specifically checks the 'History' table.

    It should be run after insert_history().
    """
    if isinstance(transformed_data, str):
        transformed_data = loads(transformed_data)

    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Prepare history data for comparison
    # dateutil.parser.parse converts `played_at` strings to datetime objects
    history_data = set(
        (
            item["user_id"],
            item["track_id"],
            parse(item["played_at"]).replace(tzinfo=None),
        )
        for item in transformed_data["History"]
    )

    # Define a helper function for querying in chunks
    def query_table_in_chunks(table: str, where_in_data: str, chunk_size: int = 1000):
        """Query a table in chunks.

        Args:
            table (str): The name of the table to query.
            where_in_data (str): WHERE clause for the SQL statement.
            chunk_size (int): The number of rows to fetch at once.

        Returns:
            Set[tuple]: The queried data.
        """
        # Get the count of relevant rows
        count_query = f'SELECT COUNT(*) FROM "{table}" WHERE {where_in_data}'
        cursor.execute(count_query)
        total_rows = cursor.fetchone()[0]

        queried_data = set()

        # Fetch and compare the data in chunks
        for offset in range(0, total_rows, chunk_size):
            query = f"""
                SELECT "user_id", "track_id", "played_at"
                FROM "{table}"
                WHERE {where_in_data}
                LIMIT {chunk_size} OFFSET {offset}
            """
            cursor.execute(query)
            queried_data.update(cursor.fetchall())

        return queried_data

    # Prepare WHERE clause
    user_ids = ", ".join(
        map(str, set(item["user_id"] for item in transformed_data["History"]))
    )
    played_at_values = ", ".join(
        f"'{item['played_at']}'" for item in transformed_data["History"]
    )
    where_in_data = f"user_id IN ({user_ids}) AND played_at IN ({played_at_values})"

    # Query History table and compare the results
    try:
        queried_history = query_table_in_chunks("History", where_in_data)
        assert history_data == queried_history, (
            "Transformed data and queried data do not match: "
            f"{history_data} != {queried_history}"
        )

    except Exception as e:
        logger.error(f"Failed to query data: {str(e)}")
        raise e

    finally:
        cursor.close()
        conn.close()

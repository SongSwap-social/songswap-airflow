"""Utility functions for transforming and loading TrackFeatures data."""

import logging
from typing import List, Optional, Set, Tuple

from airflow.providers.postgres.hooks.postgres import PostgresHook
from src.utils.rds_utils import (
    fetch_query_results_in_chunks,
    get_database_cursor,
    insert_bulk,
)
from src.utils.spotify_utils import fetch_tracks, fetch_tracks_features

logger = logging.getLogger(__name__)


def get_track_ids_from_history(history: dict) -> List[str]:
    """Get a list of track IDs from a listening history.

    Args:
        history (dict): A listening history.

    Returns:
        List[str]: A list of track IDs.
    """
    logger.info(f"Extracting track IDs from listening history.")
    track_ids = []
    for item in history["items"]:
        track_ids.append(item["track"]["id"])
    return track_ids


def fetch_tracks_data(track_ids: List[str]) -> List[dict]:
    """Fetch track data for multiple tracks from the Spotify API.

    Args:
        track_ids (List[str]): A list of track IDs.

    Returns:
        List[dict]: A list of track data.
    """
    logger.info(f"Fetching track data for {len(track_ids)} tracks.")

    # Fetch the data in max 50 track chunks
    track_data = []
    track_ids_chunks = [track_ids[i : i + 50] for i in range(0, len(track_ids), 50)]

    for idx, chunk in enumerate(track_ids_chunks):
        logger.info(f"Fetching track chunk {idx + 1} of {len(track_ids_chunks)}")
        tracks = fetch_tracks(chunk)["tracks"]
        track_data.extend(tracks)

    return track_data


def fetch_track_features_data(track_ids: List[str]) -> List[dict]:
    """Fetch track features data for multiple tracks from the Spotify API.

    Args:
        track_ids (List[str]): A list of track IDs.

    Returns:
        List[dict]: A list of track features data.
    """
    logger.info(f"Fetching track features data for {len(track_ids)} tracks.")

    # Fetch the data in max 100 track chunks
    track_features_data = []
    track_ids_chunks = [track_ids[i : i + 100] for i in range(0, len(track_ids), 100)]

    for idx, chunk in enumerate(track_ids_chunks):
        logger.info(
            f"Fetching track features chunk {idx + 1} of {len(track_ids_chunks)}"
        )
        track_features = fetch_tracks_features(chunk)
        track_features_data.extend(track_features)

    return track_features_data


def transform_track_data(raw_track_data: dict, played_at: Optional[str] = None) -> dict:
    """
    Transforms tracks retrieved from Spotify listening history data into a format compatible with `insert_track` function.

    Args:
        raw_track_data (dict): Raw track data retrieved from Spotify listening history data.
        played_at (str): Inject a played_at timestamp into the transformed data.
            NOTE: This is only used for backfilling and testing purposes.

    Returns:
        dict: Transformed data containing artists, tracks, artist_tracks, etc.
    """

    # Initialize data
    data = {
        "Artists": [],
        "Tracks": [],
        "ArtistTracks": [],
        "TrackPopularity": [],
        "TrackImages": [],
        "TrackPreviews": [],
    }

    # Determine if raw_track_data is from history or Spotify's /tracks API
    # The /tracks API returns a list of tracks in the form: [{track}]}
    # The history data returns a dictionary of tracks in the form: {"items": [{"track": {track}}]}
    if isinstance(raw_track_data, list):
        # Inject a played_at timestamp into the transformed data
        items = [
            {"track": track, "played_at": played_at}
            for track in raw_track_data
        ]
        raw_track_data = {"items": items}

    # Transform raw_track_data
    for track_played in raw_track_data["items"]:
        track_played: dict
        track: dict = track_played.get("track")

        if not track:
            continue

        artists = track.get("artists")
        album: dict = track.get("album")
        images = album.get("images")
        track_id: str = track.get("id")
        track_name: str = track.get("name")
        track_duration_ms = track.get("duration_ms")
        track_popularity = track.get("popularity")
        preview_url = track.get("preview_url")
        played_at = played_at or track_played.get("played_at")

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

        if preview_url:
            data["TrackPreviews"].append(
                {
                    "id": track_id,
                    "url": preview_url,
                }
            )

    return data


def transform_track_features_data(raw_track_features: List[dict]) -> dict:
    """Transform raw track features data into a format that can be loaded into the database.

    Args:
        raw_track_features (List[dict]): A list of raw track features data.
            :: obtained from `fetch_track_features_data()`

    Returns:
        dict: The transformed data.
    """
    data = {"TrackFeatures": []}

    logger.info(
        f"Transforming track features data for {len(raw_track_features)} tracks."
    )
    for track in raw_track_features:
        data["TrackFeatures"].append(
            {
                "id": track["id"],
                "acousticness": track["acousticness"],
                "danceability": track["danceability"],
                "energy": track["energy"],
                "instrumentalness": track["instrumentalness"],
                "key": track["key"],
                "liveness": track["liveness"],
                "loudness": track["loudness"],
                "mode": track["mode"],
                "speechiness": track["speechiness"],
                "tempo": track["tempo"],
                "time_signature": track["time_signature"],
                "valence": track["valence"],
            }
        )

    return data


def validate_transformed_track_data_keys(data: dict):
    """Verify that the transformed data contains the correct keys.

    The keys should be the names of the tables in the database, and the values
    should be lists of dictionaries, where each dictionary is a row in the table.

    Args:
        data (dict): The transformed data to be verified.
    """
    logger.info(f"Verifying transformed data keys: {data}")

    assert len(data) == 6, f"Transformed data should contain 6 tables: {data.keys()}"
    expected_table_keys = [  # Expected table names
        "Artists",
        "Tracks",
        "ArtistTracks",
        "TrackPopularity",
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


def validate_transformed_track_data_values(data: dict):
    """Verify that the transformed data contains the correct values.

    Args:
        data (dict): The transformed data to be verified.
    """
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


def validate_transformed_track_features_keys(transformed_data: dict) -> None:
    """Verify that transformed data contains all expected keys."""
    logger.info("Transformed data keys are valid.")

    # Verify that data contains all expected keys
    assert "TrackFeatures" in transformed_data, f"Missing key: TrackFeatures"
    # Verify that data does not contain any unexpected keys
    assert (
        len(transformed_data.keys()) == 1
    ), f"Unexpected keys: {transformed_data.keys()}"

    # Verify that each track contains all expected keys
    expected_columns = [
        "id",
        "acousticness",
        "danceability",
        "energy",
        "instrumentalness",
        "key",
        "liveness",
        "loudness",
        "mode",
        "speechiness",
        "tempo",
        "time_signature",
        "valence",
    ]
    for track in transformed_data["TrackFeatures"]:
        for column in expected_columns:
            assert column in track, f"Missing key: {column}"


def validate_transformed_track_features_values(transformed_data: dict) -> None:
    """Verify that transformed data values are valid."""
    for track in transformed_data["TrackFeatures"]:
        assert track["id"] is not None, f"Track ID cannot be None: {track}"
        assert (
            0 <= track["acousticness"] <= 1
        ), f"Acousticness value out of range [0, 1]: {track}"
        assert (
            0 <= track["danceability"] <= 1
        ), f"Danceability value out of range [0, 1]: {track}"
        assert 0 <= track["energy"] <= 1, f"Energy value out of range [0, 1]: {track}"
        assert (
            0 <= track["instrumentalness"] <= 1
        ), f"Instrumentalness value out of range [0, 1]: {track}"
        assert -1 <= track["key"] <= 11, f"Key value out of range [-1, 11]: {track}"
        assert (
            0 <= track["liveness"] <= 1
        ), f"Liveness value out of range [0, 1]: {track}"
        assert (
            -60 <= track["loudness"] <= 0
        ), f"Loudness value out of range [-60, 0]: {track}"
        assert track["mode"] in [0, 1], f"Mode value must be either 0 or 1: {track}"
        assert (
            0 <= track["speechiness"] <= 1
        ), f"Speechiness value out of range [0, 1]: {track}"
        assert 0 <= track["tempo"] <= 250, f"Tempo value out of range [0, 250]: {track}"
        assert (
            3 <= track["time_signature"] <= 7
        ), f"Time signature value out of range [3, 7]: {track}"
        assert 0 <= track["valence"] <= 1, f"Valence value out of range [0, 1]: {track}"

    logger.info("Transformed data values are valid.")


def insert_track_bulk(transformed_data: dict, pg_hook: PostgresHook) -> None:
    """Insert track data for multiple tracks into database

    Args:
        transformed_data (dict): The transformed data to be inserted. `data` should be
            dictionary with keys "Artists", "Tracks", "ArtistTracks", "TrackPopularity",
            "TrackImages", "TrackPreviews" and values containing a list of dictionaries
            with keys "id", "name", etc.
        pg_hook (PostgresHook): Hook to connect to the database.
    """
    logger.info("Inserting track data into database.")
    insert_bulk(transformed_data, pg_hook)


def insert_track_features_bulk(transformed_data: dict, pg_hook: PostgresHook) -> None:
    """Insert track feature data for multiple tracks into database


    Args:
        transformed_data (dict): The transformed data to be inserted. `data` should be
            dictionary with keys "TrackFeatures" and values containing a list of
            dictionaries with keys "id", "acousticness", "danceability", "energy", etc.
        pg_hook (PostgresHook): Hook to connect to the database.
    """
    logger.info("Inserting track features data into database.")
    insert_bulk(transformed_data, pg_hook)


def _prepare_track_features_data_for_comparison(transformed_data: dict) -> Set[Tuple]:
    """Prepare transformed data for comparison with data in the database.

    Args:
        transformed_data (dict): The transformed data to be inserted. `data` should be
            dictionary with keys "TrackFeatures" and values containing a list of
            dictionaries with keys "id", "acousticness", "danceability", "energy", etc.

    Returns:
        Set[Tuple]: A set of tuples (DB rows) containing the track ID and all track features.
    """
    track_features_data = transformed_data["TrackFeatures"]
    # ! NOTE: Order matters. It must match the order of the columns in the database.
    track_features_set = set(
        (
            track["id"],
            track["acousticness"],
            track["danceability"],
            track["energy"],
            track["instrumentalness"],
            track["key"],
            track["liveness"],
            track["loudness"],
            track["mode"],
            track["speechiness"],
            track["tempo"],
            track["time_signature"],
            track["valence"],
        )
        for track in track_features_data
    )
    return track_features_set


def _construct_where_clause(transformed_data: dict) -> str:
    """Construct the WHERE clause for the SELECT query."""
    track_ids = ",".join(
        [f"'{track['id']}'" for track in transformed_data["TrackFeatures"]]
    )
    where_clause = f"id IN ({track_ids})"
    return where_clause


def _construct_select_query(table_name: str, where_clause: str) -> str:
    """Construct the SELECT query."""
    select_query = f'SELECT * FROM "{table_name}" WHERE {where_clause}'
    return select_query


def _assert_matching_data(expected_data: Set[Tuple], actual_data: Set[Tuple]) -> None:
    """Assert that the expected data matches the actual data."""
    assert len(expected_data) == len(
        actual_data
    ), f"Number of rows do not match: len(expected_data)={len(expected_data)}, len(actual_data)={len(actual_data)}"
    for expected_row, actual_row in zip(expected_data, actual_data):
        assert (
            expected_row == actual_row
        ), f"Data does not match. Expected: {expected_row}, Actual: {actual_row}"


def verify_inserted_track_features(transformed_data: dict, pg_hook: PostgresHook):
    """Verify that the transformed data has been inserted into the database."""
    conn, cursor = get_database_cursor(pg_hook)

    track_features_data = _prepare_track_features_data_for_comparison(transformed_data)
    where_clause = _construct_where_clause(transformed_data)
    query = _construct_select_query("TrackFeatures", where_clause)

    try:
        queried_track_features = fetch_query_results_in_chunks(cursor, query)
        _assert_matching_data(track_features_data, queried_track_features)

    except Exception as e:
        logger.error(f"Failed to query data: {str(e)}")
        raise e

    finally:
        cursor.close()
        conn.close()

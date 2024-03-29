"""Utility functions for fetching, parsing, and saving artist data to RDS."""
import logging
from typing import List

from airflow.providers.postgres.hooks.postgres import PostgresHook
from src.utils.rds_utils import insert_bulk
from src.utils.spotify_utils import fetch_artists

logger = logging.getLogger(__name__)


def transform_data(raw_data: List[dict]) -> dict:
    """
    Transforms raw JSON Spotify data into a format compatible with `insert_artist_bulk` function.

    `raw_data` is retrieved from the `fetch_artists_data_with_dates` function.

    Args:
        raw_data (list): List of raw Artist JSON data from Spotify API /artists endpoint.

    Returns:
        dict: Transformed data containing artist information.
    """
    data = {
        "ArtistImages": [],
        "ArtistPopularity": [],
        "ArtistGenres": [],
        "ArtistFollowers": [],
    }

    for artist in raw_data:
        artist_id = artist.get("id")
        followers = artist.get("followers")
        genres = artist.get("genres")
        images = artist.get("images")
        popularity = artist.get("popularity")
        date = artist.get("date")

        data["ArtistPopularity"].append(
            {"id": artist_id, "date": date, "popularity": popularity}
        )

        data["ArtistGenres"].extend(
            [{"id": artist_id, "genre": genre} for genre in genres]
        )

        data["ArtistFollowers"].append(
            {"id": artist_id, "date": date, "followers": followers["total"]}
        )

        data["ArtistImages"].extend(
            [
                {
                    "id": artist_id,
                    "width": image["width"],
                    "height": image["height"],
                    "url": image["url"],
                }
                for image in images
            ]
        )

    return data


def get_artists_and_dates_from_history(history: List[dict]) -> List[tuple]:
    """
    Extracts artists and dates from listening history data.

    Args:
        history (list): List of raw listening history data.

    Returns:
        List[tuple]: List of tuples containing artist id and date.
    """
    logger.info(f"Extracting artists and dates from {len(history)} history items.")
    logger.info(f"HISTORY: {history}")
    artists_and_dates = [
        (artist["id"], item["played_at"])
        for item in history["items"]
        for artist in item["track"]["artists"]
    ]
    return artists_and_dates


def fetch_artists_data_with_dates(artists_and_dates: List[tuple]) -> List[dict]:
    """
    Fetches artist data from Spotify API and adds `played_at` dates to each artist.

    Args:
        artists_and_dates (list): List of tuples containing artist id and date.
            :: from `get_artists_and_dates_from_history` function.

    Returns:
        List[dict]: List of artist data dictionaries.

    """
    logger.info(f"Fetching artists data for {len(artists_and_dates)} artists.")

    # Chunk artist_and_dates into groups of 50
    chunk_size = 50
    artists_and_dates_chunks = [
        artists_and_dates[i : i + chunk_size]
        for i in range(0, len(artists_and_dates), chunk_size)
    ]

    artists_data = []
    # Fetch artist data for each chunk
    for i, chunk in enumerate(artists_and_dates_chunks):
        logger.info(f"Fetching chunk {i+1}/{len(artists_and_dates_chunks)}")
        artists = fetch_artists([artist_id for artist_id, _ in chunk])["artists"]
        # Add played_date to each artist
        # ASSUMPTION: The order of artists returned from the API is the same as the order of artist ids passed in.
        for artist, (_, played_at) in zip(artists, chunk):
            artist["date"] = played_at
        artists_data.extend(artists)

    return artists_data


def validate_assumption(artists_data: List[dict], artists_and_dates: List[tuple]):
    """
    Validates the assumption that the order of artists returned from the API is the same as the order of artist ids
    passed in.

    Verifies that the order of artist ids in `artists_data` is the same as the order of artist ids in
    `artists_and_dates`. Also verifies that the dates are the same.

    Args:
        artists_data (list): List of artist data dictionaries.
        artists_and_dates (list): List of tuples containing artist id and date.
            :: from `get_artists_and_dates_from_history` function.

    Raises:
        ValueError: If the order of artists returned from the API is not the same as the order of artist ids passed in.
    """

    logger.info(
        "Validating assumption that the order of artists returned from the API is the same as the order of "
        "artist ids passed in."
    )
    # Get artist ids and dates from artists_data
    artists_data_ids_and_dates = [
        (artist["id"], artist["date"]) for artist in artists_data
    ]

    # Check that the order of artist ids is the same
    if artists_data_ids_and_dates != artists_and_dates:
        raise ValueError(
            "The order of artists returned from the API is not the same as the order of artist ids passed in."
        )


def insert_artist_bulk(transformed_data: dict, pg_hook: PostgresHook):
    """Insert data for multiple artists into the database.

    Args:
        data_list (List[dict]): A list of data dictionaries to be inserted. Each dictionary should have keys
            corresponding to the table names and values corresponding to the data to be inserted.
        pg_hook (PostgresHook): Hook to connect to the database.
    """
    insert_bulk(transformed_data, pg_hook)


def verify_transformed_data_keys(data: dict):
    """Verifies that the keys in the data dictionary are valid.

    Args:
        data (dict): A dictionary of data to be inserted. The keys should correspond to the table names and the values
            should correspond to the data to be inserted. Expected keys are:
            - ArtistImages
            - ArtistPopularity
            - ArtistGenres
            - ArtistFollowers
    """
    expected_keys = {
        "ArtistImages",
        "ArtistPopularity",
        "ArtistGenres",
        "ArtistFollowers",
    }
    # Check for invalid keys
    invalid_keys = set(data.keys()) - expected_keys
    if invalid_keys:
        raise ValueError(
            f"Invalid key(s) in data dictionary: {invalid_keys}. Expected one of {expected_keys}."
        )

    # Check for missing keys
    missing_keys = expected_keys - set(data.keys())
    if missing_keys:
        raise ValueError(f"Missing key(s) in data dictionary: {missing_keys}.")


def verify_transformed_data_values(data: dict):
    """Verifies that the values in the data dictionary are valid.

    The input is expected to be from the `transform_data` function.

    Args:
        data (dict): A dictionary of data to be inserted. The keys should correspond to the table names and the values
            should correspond to the data to be inserted. Expected keys are:
            - ArtistImages
            - ArtistPopularity
            - ArtistGenres
            - ArtistFollowers
    """
    for table_name, row_data in data.items():
        if not isinstance(row_data, list):
            raise ValueError(
                f"Invalid value for key {table_name}. Expected list, got {type(row_data)}."
            )

        if not row_data:
            raise ValueError(
                f"Invalid value for key {table_name}. Expected non-empty list."
            )

        sample_row = row_data[0]
        # Ensure all rows are dictionaries
        if not isinstance(sample_row, dict):
            raise ValueError(
                f"Invalid value for key {table_name}. Expected list of dictionaries, got list of {type(sample_row)}."
            )

        # Ensure all rows are non-empty dictionaries
        if not sample_row:
            raise ValueError(
                f"Invalid value for key {table_name}. Expected list of non-empty dictionaries."
            )

        # Ensure all keys in a row are of type str
        if not sample_row.keys():
            raise ValueError(
                f"Invalid value for key {table_name}. Expected list of dictionaries with non-empty keys."
            )

        # Ensure all values in a row are of type str
        if not all(isinstance(row, dict) for row in row_data):
            raise ValueError(
                f"Invalid value for key {table_name}. Expected list of dictionaries, got list of {type(sample_row)}."
            )

        # Ensure all rows have the same keys
        if not all(row.keys() == sample_row.keys() for row in row_data):
            raise ValueError(
                f"Invalid value for key {table_name}. Expected list of dictionaries with same keys."
            )

        # Ensure no values in a row are empty
        if not all(row.values() for row in row_data):
            raise ValueError(
                f"Invalid value for key {table_name}. Expected list of dictionaries with non-empty values."
            )

        # Ensure no values in a row are of type dict
        if any(isinstance(value, dict) for row in row_data for value in row.values()):
            raise ValueError(
                f"Invalid value for key {table_name}. Expected list of dictionaries with no nested dictionaries."
            )

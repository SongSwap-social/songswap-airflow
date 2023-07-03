import logging
from os import environ
from typing import List

import spotipy
from airflow.providers.postgres.hooks.postgres import PostgresHook
from spotipy.oauth2 import SpotifyOAuth

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


SPOTIFY_AUTH_MGR = SpotifyOAuth(
    client_id=environ.get("SPOTIFY_CLIENT_ID"),
    client_secret=environ.get("SPOTIFY_CLIENT_SECRET"),
    redirect_uri=environ.get("SPOTIFY_REDIRECT_URI"),
    open_browser=False,
)


def fetch_tokens(pg_hook: PostgresHook) -> dict:
    """Fetch the Spotify access and refresh tokens for all users

    Args:
        pg_hook (PostgresHook): PostgresHook to connect to the database

    Returns:
        dict: The Spotify access and refresh tokens for all users
            :: {user_id: {"access_token": str, "refresh_token": str}}
    """
    tokens = {}
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            # Retrieve the current tokens for all users
            query = 'SELECT u.id, st.access_token, st.refresh_token FROM "SpotifyTokens" as st INNER JOIN "Users" as u ON u.id = st.id'

            cursor.execute(query)
            user_tokens = cursor.fetchall()
            for user_id, access_token, refresh_token in user_tokens:
                tokens[user_id] = {
                    "access_token": access_token,
                    "refresh_token": refresh_token,
                }

    return tokens


def refresh_access_token(
    user_id: int, refresh_token: str, pg_hook: PostgresHook
) -> str:
    """Refresh a user's Spotify access token and update the database

    Args:
        user_id (int): The user's SongSwap ID
        refresh_token (str): The user's Spotify refresh token
        pg_hook (PostgresHook): PostgresHook to connect to the database

    Returns:
        str: The user's new Spotify access token
    """
    new_token_info = _fetch_refreshed_access_token(refresh_token)
    new_access_token = new_token_info["access_token"]
    _set_refreshed_access_token(
        user_id=user_id, new_access_token=new_access_token, pg_hook=pg_hook
    )
    return new_access_token


def _fetch_refreshed_access_token(refresh_token: str) -> dict:
    """Fetch a new access token from Spotify using the refresh token

    Args:
        refresh_token (str): The user's Spotify refresh token

    Returns:
        dict: The user's new Spotify access token
            :: {"access_token": str, "token_type": str, "expires_in": int, "scope": str}
    """
    new_token_info = SPOTIFY_AUTH_MGR.refresh_access_token(refresh_token=refresh_token)
    return new_token_info


def _set_refreshed_access_token(
    user_id: int, new_access_token: str, pg_hook: PostgresHook
):
    """Update the access token for a user in the database

    Args:
        user_id (int): The user's SongSwap ID
        new_access_token (str): The user's new Spotify access token
        pg_hook (PostgresHook): PostgresHook to connect to the database
    """
    # Update the token in the database
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            # UPDATE SpotifyTokens SET access_token = %s WHERE id = %s
            query = 'UPDATE "SpotifyTokens" SET access_token = %s WHERE id = %s'
            cursor.execute(query, (new_access_token, user_id))
            conn.commit()


def fetch_listening_history(
    access_token: str, after: int = None, before: int = None
) -> dict:
    """Fetch the listening history for a user from Spotify.
    Optionally, provide a unix timestamp for after and/or before to filter the results.

    Args:
        access_token (str): The user's Spotify access token
        after (int, optional): Unix timestamp in milliseconds of the lower-bound hour we want to fetch the listening history for. Defaults to None.
        before (int, optional): Unix timestamp in milliseconds of the upper-bound hour we want to fetch the listening history for. Defaults to None.

    Returns:
        dict: The user's listening history
            :: {"items": list, "next": str, "cursors": dict, "limit": int, "href": str}

    """
    sp = spotipy.Spotify(auth=access_token)
    # Log the fetching of the history
    results = sp.current_user_recently_played(after=after, before=before)
    logger.info(f" Fetched history: length len(items)={len(results['items'])}")
    return results


def fetch_artists_data(artist_ids: List[str]) -> dict:
    """Fetches artist data from Spotify for a list of artist IDs.

    Args:
        artist_ids (list): List of artist IDs to fetch data for.

    Returns:
        dict: The artist data for the provided artist IDs
            :: {"artists": list }
    Raises:
        ValueError: If more than 50 artist IDs are provided

    Assumption:
        - The results are returned in the same order as the artist IDs provided.
        - The artist IDs provided are valid. The API does not return an error if an invalid ID is provided.

    Example response ::
        {
            "artists": [
                {
                "external_urls": {
                    "spotify": "https://open.spotify.com/artist/3a34v9rZzoFZ7K19NszX9F"
                },
                "followers": {
                    "href": null,
                    "total": 4596
                },
                "genres": [],
                "href": "https://api.spotify.com/v1/artists/3a34v9rZzoFZ7K19NszX9F",
                "id": "3a34v9rZzoFZ7K19NszX9F",
                "images": [
                    {
                    "height": 640,
                    "url": "https://i.scdn.co/image/ab6761610000e5ebc493aac320d788d8ff7aaceb",
                    "width": 640
                    },
                    {
                    "height": 320,
                    "url": "https://i.scdn.co/image/ab67616100005174c493aac320d788d8ff7aaceb",
                    "width": 320
                    },
                    {
                    "height": 160,
                    "url": "https://i.scdn.co/image/ab6761610000f178c493aac320d788d8ff7aaceb",
                    "width": 160
                    }
                ],
                "name": "Dave Okumu",
                "popularity": 32,
                "type": "artist",
                "uri": "spotify:artist:3a34v9rZzoFZ7K19NszX9F"
                },
                {
                "external_urls": {
                    "spotify": "https://open.spotify.com/artist/6I3MElirhT5t6Kf7p0hGk9"
                },
                "followers": {
                    "href": null,
                    "total": 421457
                },
                "genres": [
                    "alternative r&b"
                ],
                "href": "https://api.spotify.com/v1/artists/6I3MElirhT5t6Kf7p0hGk9",
                "id": "6I3MElirhT5t6Kf7p0hGk9",
                "images": [
                    {
                    "height": 640,
                    "url": "https://i.scdn.co/image/ab6761610000e5ebd6a25277f111957ed31b7e78",
                    "width": 640
                    },
                    {
                    "height": 320,
                    "url": "https://i.scdn.co/image/ab67616100005174d6a25277f111957ed31b7e78",
                    "width": 320
                    },
                    {
                    "height": 160,
                    "url": "https://i.scdn.co/image/ab6761610000f178d6a25277f111957ed31b7e78",
                    "width": 160
                    }
                ],
                "name": "Duckwrth",
                "popularity": 61,
                "type": "artist",
                "uri": "spotify:artist:6I3MElirhT5t6Kf7p0hGk9"
                }
            ]
        }
    """

    sp = spotipy.Spotify(oauth_manager=SPOTIFY_AUTH_MGR)
    # Verify there are no more than 50 artist IDs
    if len(artist_ids) > 50:
        raise ValueError("Can only fetch data for up to 50 artists at a time")

    # Fetch the artists data
    artists_data = sp.artists(artist_ids)

    logger.info(f"Fetched artists data: length len(artists_data)={len(artists_data)}")
    return artists_data

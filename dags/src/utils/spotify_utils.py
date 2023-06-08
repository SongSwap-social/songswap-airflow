import logging
from os import environ

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
            query = "SELECT users.id, spotify_tokens.access_token, spotify_tokens.refresh_token FROM spotify_tokens INNER JOIN users ON users.id = spotify_tokens.id"

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
            # UPDATE spotify_tokens SET access_token = %s WHERE id = %s
            query = "UPDATE spotify_tokens SET access_token = %s WHERE id = %s"
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

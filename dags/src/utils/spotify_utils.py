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
    new_token_info = _fetch_refreshed_access_token(refresh_token)
    new_access_token = new_token_info["access_token"]
    _set_refreshed_access_token(
        user_id=user_id, new_access_token=new_access_token, pg_hook=pg_hook
    )
    return new_access_token


def _fetch_refreshed_access_token(refresh_token: str) -> dict:
    new_token_info = SPOTIFY_AUTH_MGR.refresh_access_token(refresh_token=refresh_token)
    return new_token_info


def _set_refreshed_access_token(
    user_id: int, new_access_token: str, pg_hook: PostgresHook
):
    # Update the token in the database
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            # UPDATE spotify_tokens SET access_token = %s WHERE id = %s
            query = "UPDATE spotify_tokens SET access_token = %s WHERE id = %s"
            cursor.execute(query, (new_access_token, user_id))
            conn.commit()


def fetch_listening_history(
    user_id: int, access_token: str, after: int = None, before: int = None
) -> dict:
    """Fetches the listening history for a user from Spotify.
    Optionally, provide a unix timestamp for after and/or before to filter the results.
    """
    sp = spotipy.Spotify(auth=access_token)
    # Log the fetching of the history
    logger.info(f"Fetching listening history: user_id={user_id}")
    #
    results = sp.current_user_recently_played(after=after, before=before)
    logger.info(
        f" Fetched history: user_id={user_id}, length len(items)={len(results['items'])}"
    )
    return results

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 6, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "spotipy_test",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    tags=["songswap", "sanity"],
)


def verify_spotipy_installed():
    """Import spotipy to verify it's installed."""
    try:
        import spotipy
    except ImportError as e:
        logger.error("spotipy not installed")
        raise e


def verify_environment_variables():
    """Verify that the environment variables are set."""
    from os import environ

    assert environ.get("SPOTIFY_CLIENT_ID"), "SPOTIFY_CLIENT_ID not set"
    assert environ.get("SPOTIFY_CLIENT_SECRET"), "SPOTIFY_CLIENT_SECRET not set"
    assert environ.get("SPOTIFY_REDIRECT_URI"), "SPOTIFY_REDIRECT_URI not set"


t1 = PythonOperator(
    task_id="verify_spotipy_installed",
    python_callable=verify_spotipy_installed,
    dag=dag,
)

t2 = PythonOperator(
    task_id="verify_environment_variables",
    python_callable=verify_environment_variables,
    dag=dag,
)

t1 >> t2

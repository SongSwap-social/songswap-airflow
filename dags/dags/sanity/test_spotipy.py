from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


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
    import spotipy

    print(spotipy.__name__)


t1 = PythonOperator(
    task_id="verify_spotipy_installed",
    python_callable=verify_spotipy_installed,
    dag=dag,
)

t1

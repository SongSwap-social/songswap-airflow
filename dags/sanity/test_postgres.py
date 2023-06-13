from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 6, 9),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def test_postgres_conn():
    """Test if the Postgres connection is working."""
    hook = PostgresHook(postgres_conn_id="SongSwap_RDS")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    result = cursor.fetchone()
    return result[0] == 1


with DAG(
    "test_rds_conn",
    default_args=default_args,
    description="Test if the RDS Postgres connection is working",
    schedule_interval=timedelta(days=1),
    tags=["songswap", "sanity", "test"],
    catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id="test_postgres_conn",
        python_callable=test_postgres_conn,
    )

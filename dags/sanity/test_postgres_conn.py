from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 6, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "test_postgres_conn",
    default_args=default_args,
    description="A simple tutorial DAG",
    schedule_interval=timedelta(days=1),
    tags=["songswap", "sanity"],
)


def test_postgres_conn():
    hook = PostgresHook(postgres_conn_id="SongSwap_RDS")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    result = cursor.fetchone()
    return result[0] == 1


t1 = PythonOperator(
    task_id="test_postgres_conn",
    python_callable=test_postgres_conn,
    dag=dag,
)

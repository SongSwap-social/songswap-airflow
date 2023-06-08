from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 6, 1),
}


def check_bucket_name():
    bucket_name = Variable.get("bucket_name", default_var=None)
    if bucket_name is None:
        raise AirflowException("bucket_name variable is not set in Airflow")
    return "bucket_name", bucket_name


with DAG(
    "test_variables_exist",
    default_args=default_args,
    catchup=False,
    tags=["songswap", "sanity", "test"],
) as dag:
    check_bucket_name_task = PythonOperator(
        task_id="check_bucket_name",
        python_callable=check_bucket_name,
    )

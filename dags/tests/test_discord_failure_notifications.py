"""Test if failure alerts (e.g. failed task, DAG) are sent to Discord

A mock task instance is created and passed to the `discord_notification_callback` function
to simulate a failed task. The `discord_notification_callback` function will send a
Discord message to the `#airflow-alerts` channel in the SongSwap Discord server.

To test if the failure alert is sent to Discord during a real failure, uncomment the
`test_manual_failure_task` task and comment out the `test_failure_task` task.
"""

import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from src.utils.discord_utils import discord_notification_on_failure

logger = logging.getLogger(__name__)
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 6, 1),
}


def test_manual_failure_task():
    raise Exception("Manual test failure")


with DAG(
    dag_id="test_discord_failure_notifications",
    description="DAG to test if failure alerts are sent to Discord",
    schedule_interval=None,
    start_date=datetime(2023, 6, 1),
    catchup=False,
    tags=["songswap", "discord", "test"],
) as dag:
    # Create a mock task instance class with task_id and log_url attribute
    class MockTaskInstance:
        def __init__(self, task_id: str, dag_id: str, log_url: str):
            self.task_id = task_id
            self.log_url = log_url
            self.dag_id = dag_id

    # Create a mock task instance
    mock_task_instance = MockTaskInstance(
        task_id="test_failure_task",
        dag_id="test_discord_failure_notifications",
        log_url="https://google.com",
    )

    context = {
        "task_instance": mock_task_instance,
        "execution_date": datetime.utcnow(),
    }
    test_failure_task = PythonOperator(
        task_id="test_failure_task",
        python_callable=discord_notification_on_failure,
        op_kwargs={"context": context},
    )

    #! Uncomment this line to test if the failure alert is sent to Discord
    # test_manual_failure_task = PythonOperator(
    #     task_id="test_manual_failure_task",
    #     python_callable=test_manual_failure_task,
    #     on_failure_callback=discord_notification_callback,
    # )

    test_failure_task

"""
Verify the Dicord provider is installed and the connection or endpoint variable is set.
"""

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


def verify_discord_installed():
    """Import discord to verify it's installed."""
    try:
        import airflow.providers.discord
        from airflow.providers.discord.operators.discord_webhook import (
            DiscordWebhookHook,
        )

        logger.info("Discord provider package is installed")
    except ImportError as e:
        logger.error("Discord provider package is not installed, check Dockerfile")
        raise e


def verify_discord_connection_or_variable():
    """Verify that the Discord connection or webhook endpoint variable is set."""
    from airflow.models import Connection, Variable
    from src.utils.discord_utils import (
        FAILED_TASK_CONNECTION_ID,
        FAILED_TASK_WEBHOOK_ENDPOINT_VARIABLE,
    )

    is_connection_set = False
    is_webhook_endpoint_set = False

    try:  # Verify the connection is set
        discord_connection = Connection.get_connection_from_secrets(
            FAILED_TASK_CONNECTION_ID
        )
        logger.info(
            f"Discord connection is set: {discord_connection.host}:{discord_connection.extra}"
        )
        is_connection_set = True
    except Exception as e:
        logger.error(f"Discord connection is not set, check Airflow connections: {e}")
        pass

    try:  # Verify the webhook endpoint variable is set
        discord_webhook_endpoint = Variable.get(FAILED_TASK_WEBHOOK_ENDPOINT_VARIABLE)
        logger.info(
            f"Discord webhook endpoint variable is set: {discord_webhook_endpoint}"
        )
        is_webhook_endpoint_set = True
    except Exception as e:
        logger.error(
            f"Discord webhook endpoint variable is not set, check Airflow variables: {e}"
        )
        pass

    return any([is_connection_set, is_webhook_endpoint_set])


with DAG(
    dag_id="test_discord_provider_package",
    default_args=default_args,
    description="DAG to verify that the Discord provider is installed, the Discord connection is set OR the webhook endpoint is set.",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 1),
    catchup=False,
    tags=["songswap", "discord", "test"],
) as dag:
    verify_discord_installed = PythonOperator(
        task_id="verify_discord_installed",
        python_callable=verify_discord_installed,
    )

    verify_discord_connection = PythonOperator(
        task_id="verify_discord_connection_or_variable",
        python_callable=verify_discord_connection_or_variable,
    )

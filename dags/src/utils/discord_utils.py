import json
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow.providers.discord.operators.discord_webhook import DiscordWebhookHook

FAILED_TASK_CONNECTION_ID = "Discord_Notifier"
FAILED_TASK_WEBHOOK_ENDPOINT_VARIABLE = "discord_webhook_endpoint"


class DiscordEmbedWebhookHook(DiscordWebhookHook):
    def __init__(
        self,
        http_conn_id: str = "",
        webhook_endpoint: str = None,
        embeds: list = None,
        username: str = "Airflow Notifier",
        avatar_url: str = None,
        tts: bool = False,
        proxy: str = None,
        *args,
        **kwargs,
    ) -> None:
        """A custom DiscordWebhookHook that allows for sending embeds

        Either the http_conn_id or webhook_endpoint must be set.
        """
        self.embeds = embeds

        super().__init__(
            http_conn_id=http_conn_id,
            webhook_endpoint=webhook_endpoint,
            message="",
            username=username,
            avatar_url=avatar_url,
            tts=tts,
            proxy=proxy,
            *args,
            **kwargs,
        )

    def _build_discord_payload(self) -> str:
        """
        Construct the Discord JSON payload. All relevant parameters are combined here
        to a valid Discord JSON payload.

        :return: Discord payload (str) to send
        """
        payload: dict[str, Any] = {}

        if self.username:
            payload["username"] = self.username
        if self.avatar_url:
            payload["avatar_url"] = self.avatar_url

        payload["tts"] = self.tts
        payload["content"] = ""
        payload["embeds"] = self.embeds
        payload["attachments"] = []

        return json.dumps(payload)


def _convert_utc_to_pst(exec_date: datetime) -> datetime:
    """Convert a datetime object from UTC to PST

    Args:
        exec_date (datetime): Datetime

    Returns:
        datetime: Datetime converted from UTC to PST
    """
    return exec_date.astimezone(timezone.utc).astimezone(timezone(-timedelta(hours=7)))


def _create_failed_task_discord_embed(
    log_url: str,
    dag_id: str,
    task_id: str,
    exec_date: datetime,
    color: str = "16711680",  # RED
) -> dict:
    """Create a Discord embed object for a failed task

    Args:
        log_url (str): URL to the Airflow log for the failed task
        dag_id (str): DAG ID
        task_id (str): Task ID
        exec_date (datetime): Execution date
        color (str, optional): Color of the embed. Defaults to "16711680" (RED).

    Returns:
        Dict[str, Any]: Discord embed object
    """
    exec_date_pst = _convert_utc_to_pst(exec_date)
    embed = {
        "title": ":red_circle: Task Failed",
        "url": log_url,
        "color": color,
        "fields": [
            {"name": "Dag ID", "value": dag_id, "inline": True},
            {"name": "Task ID", "value": task_id, "inline": True},
            {
                "name": "Execution Date (UTC)",
                "value": exec_date.strftime("%Y-%m-%d %H:%M:%S"),
            },
            {
                "name": "Execution Date (PST)",
                "value": exec_date_pst.strftime("%Y-%m-%d %H:%M:%S"),
                "inline": True,
            },
        ],
    }
    return embed


def discord_notification_on_failure(context: dict):
    """Task/DAG callback function to send a Discord notification upon failure

    Pass this function to a task's or DAG's `on_failure_callback` parameter

    Args:
        context (dict): Context dictionary passed by Airflow

    Returns:
        str: The response from the Discord API
    """
    exec_date = context.get("data_interval_start")
    embed = _create_failed_task_discord_embed(
        log_url=context.get("task_instance").log_url,
        dag_id=context.get("task_instance").dag_id,
        task_id=context.get("task_instance").task_id,
        exec_date=exec_date,
    )

    failure_alert = DiscordEmbedWebhookHook(
        http_conn_id=FAILED_TASK_CONNECTION_ID, embeds=[embed]
    )
    return failure_alert.execute()

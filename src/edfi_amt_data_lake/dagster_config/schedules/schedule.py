from typing import Any

from dagster import schedule

from dagster_config.jobs.pipe_api import pipe_api_job


@schedule(cron_schedule="0 * * * *", job=pipe_api_job, execution_timezone="US/Central")
def hourly_schedule(_) -> Any:
    return {}

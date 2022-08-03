from dagster import schedule
from dagster_data_lake.jobs.pipe_api import pipe_api_job

@schedule(cron_schedule="0 * * * *", job=pipe_api_job, execution_timezone="US/Central")
def hourly_schedule(_) -> None:
    return {}

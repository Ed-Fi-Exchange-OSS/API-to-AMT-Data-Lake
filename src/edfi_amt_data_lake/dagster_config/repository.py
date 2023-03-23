from dagster import repository

from dagster_config.jobs.pipe_api import pipe_api_job
from dagster_config.schedules.schedule import hourly_schedule


@repository
def dagster_config():
    jobs = [pipe_api_job]
    schedules = [hourly_schedule]
    return jobs + schedules

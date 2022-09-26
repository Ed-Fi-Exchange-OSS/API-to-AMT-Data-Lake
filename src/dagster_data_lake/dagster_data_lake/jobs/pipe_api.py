from dagster import job

from dagster_data_lake.ops.api import api


@job
def pipe_api_job():
    api()

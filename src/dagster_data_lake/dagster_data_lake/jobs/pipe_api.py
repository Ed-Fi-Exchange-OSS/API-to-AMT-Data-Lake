from dagster import job

from dagster_data_lake.ops.api import get_api_data,generate_parquet


@job
def pipe_api_job():
    generate_parquet(get_api_data())

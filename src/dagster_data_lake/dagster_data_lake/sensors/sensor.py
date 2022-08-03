from dagster import RunRequest, sensor
from dagster_data_lake.jobs.pipe_api import pipe_api_job

@sensor(job=pipe_api_job)
def sensor(_) -> None:
    should_run = True
    if should_run:
        yield RunRequest(run_key=None, run_config={})

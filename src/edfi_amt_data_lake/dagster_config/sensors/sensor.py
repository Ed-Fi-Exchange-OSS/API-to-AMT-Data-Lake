from typing import Any

from dagster import RunRequest, sensor

from dagster_config.jobs.pipe_api import pipe_api_job


@sensor(job=pipe_api_job)
def is_sensor(_) -> Any:
    should_run = True
    if should_run:
        yield RunRequest(run_key=None, run_config={})

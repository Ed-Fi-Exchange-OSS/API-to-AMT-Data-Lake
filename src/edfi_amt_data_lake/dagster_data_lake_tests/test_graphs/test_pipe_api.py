from dagster_data_lake.jobs.pipe_api import pipe_api_job


def test_pipe_api() -> bool:
    process = pipe_api_job.execute_in_process()
    assert process.success
    return True

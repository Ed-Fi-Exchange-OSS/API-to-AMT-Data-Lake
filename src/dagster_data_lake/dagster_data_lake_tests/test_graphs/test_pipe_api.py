from dagster_data_lake.jobs.pipe_api import pipe_api_job

def test_pipe_api() -> bool:
    result = pipe_api_job.execute_in_process()
    assert result.success

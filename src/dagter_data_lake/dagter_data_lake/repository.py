from dagster import repository

from dagter_data_lake.jobs.say_hello import say_hello_job
from dagter_data_lake.schedules.my_hourly_schedule import my_hourly_schedule
from dagter_data_lake.sensors.my_sensor import my_sensor


@repository
def dagter_data_lake():
    """
    The repository definition for this dagter_data_lake Dagster repository.

    For hints on building your Dagster repository, see our documentation overview on Repositories:
    https://docs.dagster.io/overview/repositories-workspaces/repositories
    """
    jobs = [say_hello_job]
    schedules = [my_hourly_schedule]
    sensors = [my_sensor]

    return jobs + schedules + sensors

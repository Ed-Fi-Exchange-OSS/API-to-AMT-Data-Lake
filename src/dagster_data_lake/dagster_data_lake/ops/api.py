from dagster import op
from edfi_amt_data_lake.api import get_data

@op
def api() -> None:
    get_data()

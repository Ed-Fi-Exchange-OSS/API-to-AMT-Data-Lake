from dagster import op
from edfi_amt_data_lake.api.api import get_all

@op
def api() -> None:
    get_all()

from dagster import op
from edfi_amt_data_lake.api.api import get_all
from edfi_amt_data_lake.api.changeVersion import get_change_version_updated

@op
def api() -> None:
    if get_change_version_updated():
        get_all()

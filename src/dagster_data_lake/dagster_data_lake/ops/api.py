from dagster import op
from edfi_amt_data_lake.api.api import get_all
from edfi_amt_data_lake.api.changeVersion import get_change_version_updated
from edfi_amt_data_lake.parquet.amt import run
from edfi_amt_data_lake.helper.helper import get_school_year

@op
def api() -> None:
    for school_year in get_school_year():
        if get_change_version_updated(school_year):
            get_all(school_year)
        run()
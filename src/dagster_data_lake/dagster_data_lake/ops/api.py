# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from dagster import op

from edfi_amt_data_lake.api.api import get_all
from edfi_amt_data_lake.api.changeVersion import get_change_version_updated
from edfi_amt_data_lake.helper.helper import get_school_year
from edfi_amt_data_lake.parquet.amt_parquet import generate_amt_parquet


@op
def get_api_data() -> None:
    for school_year in get_school_year():
        if get_change_version_updated(school_year):
            get_all(school_year)
    return True


@op
def generate_parquet(api_result_sucess) -> None:
    if(api_result_sucess):
        for school_year in get_school_year():
            generate_amt_parquet(school_year)

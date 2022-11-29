# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from edfi_amt_data_lake.helper.data_frame_generation_result import (
    data_frame_generation_result,
)
from edfi_amt_data_lake.parquet.amt.base.grading_period_dim.main import (
    grading_period_dim,
)
from edfi_amt_data_lake.parquet.Common.pandasWrapper import create_parquet_file
RESULT_COLUMNS = [
    'GradingPeriodKey',
    'GradingPeriodBeginDateKey',
    'GradingPeriodEndDateKey',
    'GradingPeriodDescription',
    'TotalInstructionalDays',
    'PeriodSequence',
    'SchoolKey',
    'SchoolYear'
]


@create_parquet_file
def most_recent_grading_period_dataframe(
    file_name: str,
    columns: list[str],
    school_year: int
):
    file_name = file_name
    columns = columns
    result_data_frame = grading_period_dim(school_year).data_frame
    if result_data_frame is None:
        return None
    result_data_frame = result_data_frame.groupby(['schoolKey'], sort=False)['gradingPeriodBeginDateKey'].max()
    result_data_frame = result_data_frame.to_frame()
    return result_data_frame


def most_recent_grading_period(school_year) -> data_frame_generation_result:
    return most_recent_grading_period_dataframe(
        file_name="mostRecentGradingPeriod.parquet",
        columns=RESULT_COLUMNS,
        school_year=school_year
    )

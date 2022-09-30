# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from decouple import config
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    saveParquetFile
)
from edfi_amt_data_lake.parquet.amt.base.grading_period_dim.main import (
    grading_period_dim_dataframe
)


def most_recent_grading_period(school_year) -> None:
    result_data_frame = grading_period_dim_dataframe(school_year)

    result_data_frame = result_data_frame.groupby(['schoolKey'], sort=False)['gradingPeriodBeginDateKey'].max()
    result_data_frame = result_data_frame.to_frame()

    saveParquetFile(result_data_frame, f"{config('PARQUET_FILES_LOCATION')}", "mostRecentGradingPeriod.parquet", school_year)

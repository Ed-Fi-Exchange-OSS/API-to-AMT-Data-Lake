# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from decouple import config

from edfi_amt_data_lake.helper.data_frame_generation_result import (
    data_frame_generation_result,
)
from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    create_parquet_file,
    jsonNormalize,
    renameColumns,
    subset,
    to_datetime,
    to_datetime_key,
    replace_null
)

ENDPOINT_CALENDAR_DATES = 'calendarDates'
RESULT_COLUMNS = [
    'DateKey',
    'Date',
    'Day',
    'Month',
    'MonthName',
    'CalendarQuarter',
    'CalendarQuarterName',
    'Year',
    'SchoolYear'
]


@create_parquet_file
def date_dim_data_frame(
    file_name: str,
    columns: list[str],
    school_year: int
):
    file_name = file_name
    calendar_date_content = getEndpointJson(ENDPOINT_CALENDAR_DATES, config('SILVER_DATA_LOCATION'), school_year)
    ############################
    # calendarDates
    ############################
    result_data_frame = jsonNormalize(
        calendar_date_content,
        recordPath=None,
        meta=[
            'date',
            'calendarReference.schoolYear'
        ],
        metaPrefix=None,
        recordPrefix='calendar_date_',
        errors='ignore'
    )
    result_data_frame = renameColumns(result_data_frame, {
        'calendarReference.schoolYear': 'schoolYear'
    })
    # Select needed columns.
    result_data_frame = subset(result_data_frame, [
        'date',
        'schoolYear'
    ])
    if result_data_frame is None:
        return None
    result_data_frame.drop_duplicates()
    result_data_frame['dateKey'] = to_datetime_key(result_data_frame, 'date')
    result_data_frame['date'] = to_datetime(result_data_frame, 'date')
    result_data_frame['day'] = result_data_frame['date'].dt.day
    result_data_frame['month'] = result_data_frame['date'].dt.month
    result_data_frame['monthName'] = result_data_frame['date'].dt.month_name()
    result_data_frame['year'] = result_data_frame['date'].dt.year
    result_data_frame['calendarQuarter'] = result_data_frame['date'].dt.quarter
    result_data_frame['calendarQuarterName'] = result_data_frame['calendarQuarter'].map(
        {1: 'First', 2: 'Second', 3: 'Third', 4: 'Fourth'})
    result_data_frame = renameColumns(
        result_data_frame,
        {
            'dateKey': 'DateKey',
            'date': 'Date',
            'day': 'Day',
            'month': 'Month',
            'monthName': 'MonthName',
            'calendarQuarter': 'CalendarQuarter',
            'calendarQuarterName': 'CalendarQuarterName',
            'year': 'Year',
            'schoolYear': 'SchoolYear'
        }
    )
    replace_null(result_data_frame, 'SchoolYear', 'Unknown')
    result_data_frame['Day'] = result_data_frame['Day'].astype(str)
    result_data_frame['Month'] = result_data_frame['Month'].astype(str)
    result_data_frame['CalendarQuarter'] = result_data_frame['CalendarQuarter'].astype(str)
    result_data_frame['Year'] = result_data_frame['Year'].astype(str)
    result_data_frame['SchoolYear'] = result_data_frame['SchoolYear'].astype(str)
    # Select needed columns.
    result_data_frame = subset(result_data_frame, columns)
    return result_data_frame


def date_dim(school_year) -> data_frame_generation_result:
    return date_dim_data_frame(
        file_name="dateDim.parquet",
        columns=RESULT_COLUMNS,
        school_year=school_year
    )

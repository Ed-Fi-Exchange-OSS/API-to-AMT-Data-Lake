# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from decouple import config

from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    jsonNormalize,
    renameColumns,
    saveParquetFile,
    subset,
    to_datetime,
    to_datetime_key,
)

ENDPOINT_CALENDAR_DATES = 'calendarDates'


def date_dim(school_year) -> None:
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
    # Select needed columns.
    result_data_frame = subset(result_data_frame, [
        'dateKey',
        'date',
        'day',
        'month',
        'monthName',
        'calendarQuarter',
        'calendarQuarterName',
        'year',
        'schoolYear'
    ])

    saveParquetFile(result_data_frame, f"{config('PARQUET_FILES_LOCATION')}", "dateDim.parquet", school_year)

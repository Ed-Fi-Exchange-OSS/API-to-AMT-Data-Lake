# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from datetime import date

import pandas as pd
from decouple import config

from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    addColumnIfNotExists,
    create_parquet_file,
    get_descriptor_code_value_from_uri,
    get_reference_from_href,
    jsonNormalize,
    pdMerge,
    renameColumns,
    replace_null,
    subset,
    to_datetime_key,
    create_empty_data_frame,
    toCsv
)

ENDPOINT_STUDENT_SCHOOL_ASSOCIATION = 'studentSchoolAssociations'
ENDPOINT_STUDENT_SECTION_ASSOCIATION = 'studentSectionAssociations'
ENDPOINT_STUDENT_SCHOOL_ATTENDANCE_EVENTS = 'studentSchoolAttendanceEvents'
ENDPOINT_STUDENT_SECTION_ATTENDANCE_EVENTS = 'studentSectionAttendanceEvents'
ENDPOINT_CALENDAR_DATES = 'calendarDates'

RESULT_COLUMNS = [
    'StudentSchoolKey',
    'StudentKey',
    'SchoolKey',
    'DateKey',
    'ReportedAsPresentAtSchool',
    'ReportedAsAbsentFromSchool',
    'ReportedAsPresentAtHomeRoom',
    'ReportedAsAbsentFromHomeRoom',
    'ReportedAsIsPresentInAllSections',
    'ReportedAsAbsentFromAnySection'
]


@create_parquet_file
def chronic_absenteeism_attendance_fact_dataframe(
    file_name: str,
    columns: list[str],
    school_year: int
) -> pd.DataFrame:
    student_school_associations_content = getEndpointJson(ENDPOINT_STUDENT_SCHOOL_ASSOCIATION, config('SILVER_DATA_LOCATION'), school_year)
    student_section_associations_content = getEndpointJson(ENDPOINT_STUDENT_SECTION_ASSOCIATION, config('SILVER_DATA_LOCATION'), school_year)
    student_school_attendance_events_content = getEndpointJson(ENDPOINT_STUDENT_SCHOOL_ATTENDANCE_EVENTS, config('SILVER_DATA_LOCATION'), school_year)
    student_section_attendance_events_content = getEndpointJson(ENDPOINT_STUDENT_SECTION_ATTENDANCE_EVENTS, config('SILVER_DATA_LOCATION'), school_year)
    calendar_dates_content = getEndpointJson(ENDPOINT_CALENDAR_DATES, config('SILVER_DATA_LOCATION'), school_year)

    student_school_associations_normalize = jsonNormalize(
        student_school_associations_content,
        recordPath=None,
        meta=[
            'id',
            'entryDate',
            'ExitWithdrawDate',
            ['calendarReference', 'calendarCode'],
            ['schoolReference', 'schoolId'],
            ['schoolYearTypeReference', 'schoolYear'],
            ['studentReference', 'studentUniqueId']
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    if student_school_associations_normalize.empty:
        return None
    
    calendar_dates_normalize = jsonNormalize(
        calendar_dates_content,
        recordPath=None,
        meta=[
            'id',
            'date',
            ['calendarReference', 'calendarCode']
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    toCsv(calendar_dates_normalize, 'C:/temp/edfi/parquet/', 'calendar_dates_normalize1.csv', '')

    if calendar_dates_normalize.empty:
        return None
    
    calendar_dates_calendar_events_normalize = jsonNormalize(
        calendar_dates_content,
        recordPath=['calendarEvents'],
        meta=['id'],
        recordMeta=['calendarEventDescriptor'],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    toCsv(calendar_dates_calendar_events_normalize, 'C:/temp/edfi/parquet/', 'calendar_dates_calendar_events_normalize.csv', '')

    calendar_dates_normalize = pdMerge(
        left=calendar_dates_normalize,
        right=calendar_dates_calendar_events_normalize,
        how='left',
        leftOn=['id'],
        rightOn=['id'],
        suffixLeft=None,
        suffixRight=None
    )

    toCsv(calendar_dates_normalize, 'C:/temp/edfi/parquet/', 'calendar_dates_normalize.csv', '')

    result_data_frame = calendar_dates_normalize
    
    return result_data_frame


def chronic_absenteeism_attendance_fact(school_year) -> None:
    return chronic_absenteeism_attendance_fact_dataframe(
        file_name="chrab_chronicAbsenteeismAttendanceFact.parquet",
        columns=RESULT_COLUMNS,
        school_year=school_year
    )

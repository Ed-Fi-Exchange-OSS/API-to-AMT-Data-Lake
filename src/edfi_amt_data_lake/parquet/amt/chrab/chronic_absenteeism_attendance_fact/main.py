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
            'exitWithdrawDate',
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
            ['calendarReference', 'calendarCode'],
            ['calendarReference', 'schoolId'],
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

    # - CalendarDateCalendarEvent

    student_school_associations_normalize['_calendar_dates'] = '|'

    result_data_frame = pdMerge(
        left=student_school_associations_normalize,
        right=calendar_dates_normalize,
        how='inner',
        leftOn=['schoolReference.schoolId'],
        rightOn=['calendarReference.schoolId'],
        suffixLeft=None,
        suffixRight='_calendar_dates'
    )

    result_data_frame['entryDate_key'] = to_datetime_key(result_data_frame, 'entryDate')
    result_data_frame['date_key'] = to_datetime_key(result_data_frame, 'date')
    result_data_frame = result_data_frame[result_data_frame['entryDate_key'] <= result_data_frame['date_key']]

    result_data_frame['exitWithdrawDate_key'] = to_datetime_key(result_data_frame, 'exitWithdrawDate')
    replace_null(result_data_frame, 'exitWithdrawDate', '')
    result_data_frame = result_data_frame[(
        (result_data_frame['exitWithdrawDate'] == '') 
        | (result_data_frame['exitWithdrawDate_key'] >= result_data_frame['date_key'])
    )]

    toCsv(result_data_frame, 'C:/temp/edfi/parquet/', 'result_data_frame1.csv', '')

    # - END

    # - StudentSchoolAttendanceEvent
    
    student_school_attendance_events_normalize = jsonNormalize(
        student_school_attendance_events_content,
        recordPath=None,
        meta=[
            'id',
            'eventDate',
            'attendanceEventCategoryDescriptor',
            ['schoolReference', 'schoolId'],
            ['studentReference', 'studentUniqueId'],
            ['sessionReference', 'schoolYear']
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    student_school_attendance_events_normalize['eventDate_key'] = to_datetime_key(student_school_attendance_events_normalize, 'eventDate')

    toCsv(student_school_attendance_events_normalize, 'C:/temp/edfi/parquet/', 'student_school_attendance_events_normalize.csv', '')

    result_data_frame['_student_school_attendance_events'] = '|'

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=student_school_attendance_events_normalize,
        how='left',
        leftOn=['schoolReference.schoolId', 'studentReference.studentUniqueId', 'date_key'],
        rightOn=['schoolReference.schoolId', 'studentReference.studentUniqueId', 'eventDate_key'],
        suffixLeft=None,
        suffixRight='_student_school_attendance_events'
    )

    replace_null(result_data_frame, 'schoolYearTypeReference.schoolYear', '')
    result_data_frame['schoolYearTypeReference.schoolYear_key'] = to_datetime_key(result_data_frame, 'schoolYearTypeReference.schoolYear')

    replace_null(result_data_frame, 'sessionReference.schoolYear', '')
    result_data_frame['sessionReference.schoolYear_key'] = to_datetime_key(result_data_frame, 'sessionReference.schoolYear')

    result_data_frame = result_data_frame[(
        (result_data_frame['schoolYearTypeReference.schoolYear'] == '') 
        | (result_data_frame['schoolYearTypeReference.schoolYear_key'] == result_data_frame['sessionReference.schoolYear_key'])
    )]

    toCsv(result_data_frame, 'C:/temp/edfi/parquet/', 'result_data_frame.csv', '')

    # - END

    return result_data_frame


def chronic_absenteeism_attendance_fact(school_year) -> None:
    return chronic_absenteeism_attendance_fact_dataframe(
        file_name="chrab_chronicAbsenteeismAttendanceFact.parquet",
        columns=RESULT_COLUMNS,
        school_year=school_year
    )

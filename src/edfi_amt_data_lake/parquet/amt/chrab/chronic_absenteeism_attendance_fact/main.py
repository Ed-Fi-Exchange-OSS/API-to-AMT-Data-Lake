# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from datetime import date

import pandas as pd
from decouple import config

from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.descriptor_mapping import get_descriptor_constant
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
    crossTab,
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

    calendar_dates_normalize = pdMerge(
        left=calendar_dates_normalize,
        right=calendar_dates_calendar_events_normalize,
        how='left',
        leftOn=['id'],
        rightOn=['id'],
        suffixLeft=None,
        suffixRight=None
    )

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

    # - END

    # --- School attendance BEGIN

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

    student_school_attendance_events_normalize = get_descriptor_constant(student_school_attendance_events_normalize, 'attendanceEventCategoryDescriptor')

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

    result_data_frame_attendance_events = result_data_frame[['studentReference.studentUniqueId','schoolReference.schoolId','date_key','attendanceEventCategoryDescriptor_constantName']]

    toCsv(result_data_frame_attendance_events, 'C:/temp/edfi/parquet/', 'result_data_frame_attendance_events1.csv', '')

    result_data_frame_attendance_events = get_descriptor_constant(result_data_frame_attendance_events, 'behaviorDescriptor')
    result_data_frame_attendance_events = crossTab(
        index=[
            result_data_frame_attendance_events['studentReference.studentUniqueId'],
            result_data_frame_attendance_events['schoolReference.schoolId'],
            result_data_frame_attendance_events['date_key']
        ],
        columns=result_data_frame_attendance_events['attendanceEventCategoryDescriptor_constantName']).reset_index()

    toCsv(result_data_frame_attendance_events, 'C:/temp/edfi/parquet/', 'result_data_frame_attendance_events2.csv', '')

    result_data_frame['_attendance_events'] = '|'

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=result_data_frame_attendance_events,
        how='left',
        leftOn=['schoolReference.schoolId', 'studentReference.studentUniqueId', 'date_key'],
        rightOn=['schoolReference.schoolId', 'studentReference.studentUniqueId', 'date_key'],
        suffixLeft=None,
        suffixRight='_attendance_events'
    )

    # --- School attendance END

    # --- Section attendance BEGIN

    

    # --- Section attendance END

    result_data_frame = subset(result_data_frame, [
        'id',
        'schoolReference.schoolId',
        'schoolYearTypeReference.schoolYear',
        'studentReference.studentUniqueId',
        '_calendar_dates',
        'date',
        'calendarReference.calendarCode_calendar_dates',
        'calendarEventDescriptor',
        'date_key',
        '_student_school_attendance_events',
        'attendanceEventCategoryDescriptor_constantName',
        'attendanceEventCategoryDescriptor_codeValue_data',
        '_attendance_events',
        'AttendanceEvent.Absence',
        'AttendanceEvent.ExcusedAbsence',
        'AttendanceEvent.UnexcusedAbsence'
    ])

    toCsv(result_data_frame, 'C:/temp/edfi/parquet/', 'result_data_frame.csv', '')

    return result_data_frame


def chronic_absenteeism_attendance_fact(school_year) -> None:
    return chronic_absenteeism_attendance_fact_dataframe(
        file_name="chrab_chronicAbsenteeismAttendanceFact.parquet",
        columns=RESULT_COLUMNS,
        school_year=school_year
    )

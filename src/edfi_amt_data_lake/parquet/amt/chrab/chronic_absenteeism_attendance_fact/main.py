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

    # if student_school_associations_normalize.empty:
    #     return None
    
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

    toCsv(calendar_dates_normalize, 'C:/temp/edfi/parquet/', 'calendar_dates_normalize.csv', '')

    # - CalendarDateCalendarEvent

    # student_school_associations_normalize['_calendar_dates'] = '|'

    # result_data_frame = pdMerge(
    #     left=student_school_associations_normalize,
    #     right=calendar_dates_normalize,
    #     how='inner',
    #     leftOn=['schoolReference.schoolId'],
    #     rightOn=['calendarReference.schoolId'],
    #     suffixLeft=None,
    #     suffixRight='_calendar_dates'
    # )

    # result_data_frame['entryDate_key'] = to_datetime_key(result_data_frame, 'entryDate')
    # result_data_frame['date_key'] = to_datetime_key(result_data_frame, 'date')
    # result_data_frame = result_data_frame[result_data_frame['entryDate_key'] <= result_data_frame['date_key']]

    # result_data_frame['exitWithdrawDate_key'] = to_datetime_key(result_data_frame, 'exitWithdrawDate')
    # replace_null(result_data_frame, 'exitWithdrawDate', '')
    # result_data_frame = result_data_frame[(
    #     (result_data_frame['exitWithdrawDate'] == '') 
    #     | (result_data_frame['exitWithdrawDate_key'] >= result_data_frame['date_key'])
    # )]

    # - END

    # --- School attendance BEGIN

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

    student_attendance_events = jsonNormalize(
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

    student_attendance_events['eventDate_key'] = to_datetime_key(student_attendance_events, 'eventDate')

    student_attendance_events = get_descriptor_constant(student_attendance_events, 'attendanceEventCategoryDescriptor')

    attendance_events = (
        student_attendance_events[[
            'studentReference.studentUniqueId','schoolReference.schoolId','eventDate_key','attendanceEventCategoryDescriptor_constantName'
        ]]
    )

    attendance_events = get_descriptor_constant(attendance_events, 'behaviorDescriptor')
    attendance_events = crossTab(
        index=[
            attendance_events['studentReference.studentUniqueId'],
            attendance_events['schoolReference.schoolId'],
            attendance_events['eventDate_key']
        ],
        columns=attendance_events['attendanceEventCategoryDescriptor_constantName']).reset_index()

    addColumnIfNotExists(attendance_events, 'AttendanceEvent.Present', 0)
    addColumnIfNotExists(attendance_events, 'AttendanceEvent.Absence', 0)
    
    student_attendance_events['_attendance_events_school'] = '|'

    student_attendance_events = pdMerge(
        left=student_attendance_events,
        right=attendance_events,
        how='left',
        leftOn=['schoolReference.schoolId', 'studentReference.studentUniqueId', 'eventDate_key'],
        rightOn=['schoolReference.schoolId', 'studentReference.studentUniqueId', 'eventDate_key'],
        suffixLeft=None,
        suffixRight='_attendance_events_school'
    )

    student_schools_attendance_events = pdMerge(
        left=student_school_associations_normalize,
        right=student_attendance_events,
        how='left',
        leftOn=[
            'studentReference.studentUniqueId',
            'schoolReference.schoolId'
        ],
        rightOn=[
            'studentReference.studentUniqueId',
            'schoolReference.schoolId'
        ],
        suffixLeft=None,
        suffixRight='_attendance_events_section'
    )

    replace_null(student_schools_attendance_events, 'schoolYearTypeReference.schoolYear', '')
    student_schools_attendance_events['schoolYearTypeReference.schoolYear_key'] = to_datetime_key(student_schools_attendance_events, 'schoolYearTypeReference.schoolYear')

    replace_null(student_schools_attendance_events, 'sessionReference.schoolYear', '')
    student_schools_attendance_events['sessionReference.schoolYear_key'] = to_datetime_key(student_schools_attendance_events, 'sessionReference.schoolYear')

    student_schools_attendance_events = student_schools_attendance_events[(
        (student_schools_attendance_events['schoolYearTypeReference.schoolYear'] == '') 
        | (student_schools_attendance_events['schoolYearTypeReference.schoolYear_key'] == student_schools_attendance_events['sessionReference.schoolYear_key'])
    )]

    student_schools_attendance_events = student_schools_attendance_events[[
        'entryDate',
        'exitWithdrawDate',
        'schoolReference.schoolId',
        'schoolYearTypeReference.schoolYear',
        'studentReference.studentUniqueId',
        'attendanceEventCategoryDescriptor_constantName',
        'eventDate',
        'sessionReference.schoolYear',
        'eventDate_key',
        'attendanceEventCategoryDescriptor_codeValue_data',
        '_attendance_events_school',
        'AttendanceEvent.Absence',
        'AttendanceEvent.Present',
        'schoolYearTypeReference.schoolYear_key',
        'sessionReference.schoolYear_key'
    ]]

    toCsv(student_schools_attendance_events, 'C:/temp/edfi/parquet/', 'student_schools_attendance_events.csv', '')

    # result_data_frame['_student_school_attendance_events'] = '|'

    # result_data_frame = pdMerge(
    #     left=result_data_frame,
    #     right=student_attendance_events,
    #     how='left',
    #     leftOn=['schoolReference.schoolId', 'studentReference.studentUniqueId', 'date_key'],
    #     rightOn=['schoolReference.schoolId', 'studentReference.studentUniqueId', 'eventDate_key'],
    #     suffixLeft=None,
    #     suffixRight='_student_school_attendance_events'
    # )

    # --- School attendance END

    # --- Section attendance BEGIN

    student_section_associations_normalize = jsonNormalize(
        student_section_associations_content,
        recordPath=None,
        meta=[
            'id',
            'homeroomIndicator',
            ['studentReference', 'studentUniqueId'],
            ['sectionReference', 'localCourseCode'],
            ['sectionReference', 'schoolId'],
            ['sectionReference', 'schoolYear'],
            ['sectionReference', 'sectionIdentifier'],
            ['sectionReference', 'sessionName']
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    student_attendance_events = jsonNormalize(
        student_section_attendance_events_content,
        recordPath=None,
        meta=[
            'id',
            'eventDate',
            'attendanceEventCategoryDescriptor',
            ['studentReference', 'studentUniqueId'],
            ['sectionReference', 'localCourseCode'],
            ['sectionReference', 'schoolId'],
            ['sectionReference', 'schoolYear'],
            ['sectionReference', 'sectionIdentifier'],
            ['sectionReference', 'sessionName'],
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    student_attendance_events['eventDate_key'] = to_datetime_key(student_attendance_events, 'eventDate')

    student_attendance_events = get_descriptor_constant(student_attendance_events, 'attendanceEventCategoryDescriptor')

    attendance_events = (
        student_attendance_events[[
            'studentReference.studentUniqueId','sectionReference.schoolId','eventDate_key','attendanceEventCategoryDescriptor_constantName'
        ]]
    )

    attendance_events = get_descriptor_constant(attendance_events, 'behaviorDescriptor')
    attendance_events = crossTab(
        index=[
            attendance_events['studentReference.studentUniqueId'],
            attendance_events['sectionReference.schoolId'],
            attendance_events['eventDate_key']
        ],
        columns=attendance_events['attendanceEventCategoryDescriptor_constantName']).reset_index()

    addColumnIfNotExists(attendance_events, 'AttendanceEvent.Present', 0)
    addColumnIfNotExists(attendance_events, 'AttendanceEvent.Absence', 0)
    
    student_attendance_events['_attendance_events_section'] = '|'

    student_attendance_events = pdMerge(
        left=student_attendance_events,
        right=attendance_events,
        how='left',
        leftOn=['sectionReference.schoolId', 'studentReference.studentUniqueId', 'eventDate_key'],
        rightOn=['sectionReference.schoolId', 'studentReference.studentUniqueId', 'eventDate_key'],
        suffixLeft=None,
        suffixRight='_attendance_events_section'
    )

    student_sections_attendance_events = pdMerge(
        left=student_section_associations_normalize,
        right=student_attendance_events,
        how='left',
        leftOn=[
            'studentReference.studentUniqueId',
            'sectionReference.localCourseCode',
            'sectionReference.schoolId',
            'sectionReference.schoolYear',
            'sectionReference.sectionIdentifier',
            'sectionReference.sessionName'
        ],
        rightOn=[
            'studentReference.studentUniqueId',
            'sectionReference.localCourseCode',
            'sectionReference.schoolId',
            'sectionReference.schoolYear',
            'sectionReference.sectionIdentifier',
            'sectionReference.sessionName'
        ],
        suffixLeft=None,
        suffixRight='_attendance_events_section'
    )

    student_sections_attendance_events = student_sections_attendance_events[[
        'homeroomIndicator',
        'studentReference.studentUniqueId',
        'sectionReference.localCourseCode',
        'sectionReference.schoolId',
        'sectionReference.schoolYear',
        'sectionReference.sectionIdentifier',
        'sectionReference.sessionName',
        'attendanceEventCategoryDescriptor_constantName',
        'eventDate',
        'eventDate_key',
        '_attendance_events_section',
        'AttendanceEvent.Absence',
        'AttendanceEvent.Present'
    ]]

    toCsv(student_sections_attendance_events, 'C:/temp/edfi/parquet/', 'student_sections_attendance_events.csv', '')

    result_data_frame = student_sections_attendance_events

    # result_data_frame['_student_section_attendance_events'] = '|'

    # result_data_frame = pdMerge(
    #     left=result_data_frame,
    #     right=student_attendance_events,
    #     how='left',
    #     leftOn=['schoolReference.schoolId', 'studentReference.studentUniqueId', 'date_key'],
    #     rightOn=['schoolReference.schoolId', 'studentReference.studentUniqueId', 'eventDate_key'],
    #     suffixLeft=None,
    #     suffixRight='_student_school_attendance_events'
    # )

    # replace_null(result_data_frame, 'schoolYearTypeReference.schoolYear', '')
    # result_data_frame['schoolYearTypeReference.schoolYear_key'] = to_datetime_key(result_data_frame, 'schoolYearTypeReference.schoolYear')

    # replace_null(result_data_frame, 'sessionReference.schoolYear', '')
    # result_data_frame['sessionReference.schoolYear_key'] = to_datetime_key(result_data_frame, 'sessionReference.schoolYear')

    # result_data_frame = result_data_frame[(
    #     (result_data_frame['schoolYearTypeReference.schoolYear'] == '') 
    #     | (result_data_frame['schoolYearTypeReference.schoolYear_key'] == result_data_frame['sectionReference.schoolYear'])
    # )]


    # ------- Section attendance END

    # result_data_frame = subset(result_data_frame, [
    #     'id',
    #     'schoolReference.schoolId',
    #     'schoolYearTypeReference.schoolYear',
    #     'studentReference.studentUniqueId',
    #     '_calendar_dates',
    #     'date',
    #     'calendarReference.calendarCode_calendar_dates',
    #     'calendarEventDescriptor',
    #     'date_key',
    #     '_student_school_attendance_events',
    #     'attendanceEventCategoryDescriptor_constantName',
    #     'attendanceEventCategoryDescriptor_codeValue_data',
    #     '_attendance_events',
    #     'AttendanceEvent.Absence',
    #     'AttendanceEvent.ExcusedAbsence',
    #     'AttendanceEvent.UnexcusedAbsence'
    # ])

    toCsv(result_data_frame, 'C:/temp/edfi/parquet/', 'result_data_frame.csv', '')

    return result_data_frame


def chronic_absenteeism_attendance_fact(school_year) -> None:
    return chronic_absenteeism_attendance_fact_dataframe(
        file_name="chrab_chronicAbsenteeismAttendanceFact.parquet",
        columns=RESULT_COLUMNS,
        school_year=school_year
    )

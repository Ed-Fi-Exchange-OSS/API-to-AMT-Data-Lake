# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from datetime import date

import pandas as pd
from decouple import config

from edfi_amt_data_lake.parquet.Common.descriptor_mapping import get_descriptor_constant
from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    addColumnIfNotExists,
    create_parquet_file,
    crossTab,
    get_descriptor_code_value_from_uri,
    jsonNormalize,
    pdMerge,
    renameColumns,
    replace_null,
    subset,
    to_datetime_key,
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

    if calendar_dates_calendar_events_normalize.empty:
        return None

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

    result_data_frame['entryDate'] = to_datetime_key(result_data_frame, 'entryDate')
    result_data_frame['date'] = to_datetime_key(result_data_frame, 'date')
    result_data_frame = result_data_frame[result_data_frame['entryDate'] <= result_data_frame['date']]

    replace_null(result_data_frame, 'exitWithdrawDate', '')
    result_data_frame['exitWithdrawDate'] = to_datetime_key(result_data_frame, 'exitWithdrawDate')
    result_data_frame = result_data_frame[(
        (result_data_frame['exitWithdrawDate'] == '')
        | (result_data_frame['exitWithdrawDate'] >= result_data_frame['date'])
    )]

    # --- School attendance

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

    student_attendance_events['eventDate'] = to_datetime_key(student_attendance_events, 'eventDate')

    student_attendance_events = get_descriptor_constant(student_attendance_events, 'attendanceEventCategoryDescriptor')

    attendance_events = (
        student_attendance_events[[
            'studentReference.studentUniqueId', 'schoolReference.schoolId', 'eventDate', 'attendanceEventCategoryDescriptor_constantName'
        ]]
    )

    attendance_events = get_descriptor_constant(attendance_events, 'behaviorDescriptor')
    attendance_events = crossTab(
        index=[
            attendance_events['studentReference.studentUniqueId'],
            attendance_events['schoolReference.schoolId'],
            attendance_events['eventDate']
        ],
        columns=attendance_events['attendanceEventCategoryDescriptor_constantName']).reset_index()

    addColumnIfNotExists(attendance_events, 'AttendanceEvent.Present', 0)
    addColumnIfNotExists(attendance_events, 'AttendanceEvent.Absence', 0)

    student_attendance_events['_attendance_events_school'] = '|'

    student_attendance_events = pdMerge(
        left=student_attendance_events,
        right=attendance_events,
        how='left',
        leftOn=['schoolReference.schoolId', 'studentReference.studentUniqueId', 'eventDate'],
        rightOn=['schoolReference.schoolId', 'studentReference.studentUniqueId', 'eventDate'],
        suffixLeft=None,
        suffixRight='_attendance_events_school'
    )

    result_data_frame['_student_school_attendance_events'] = '|'

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=student_attendance_events,
        how='left',
        leftOn=['schoolReference.schoolId', 'studentReference.studentUniqueId', 'date'],
        rightOn=['schoolReference.schoolId', 'studentReference.studentUniqueId', 'eventDate'],
        suffixLeft=None,
        suffixRight='_student_school_attendance_events'
    )

    replace_null(result_data_frame, 'schoolYearTypeReference.schoolYear', '')
    result_data_frame['schoolYearTypeReference.schoolYear'] = to_datetime_key(result_data_frame, 'schoolYearTypeReference.schoolYear')

    replace_null(result_data_frame, 'sessionReference.schoolYear', '')
    result_data_frame['sessionReference.schoolYear'] = to_datetime_key(result_data_frame, 'sessionReference.schoolYear')

    result_data_frame = result_data_frame[(
        (result_data_frame['schoolYearTypeReference.schoolYear'] == '')
        | (result_data_frame['schoolYearTypeReference.schoolYear'] == result_data_frame['sessionReference.schoolYear'])
        | (result_data_frame['sessionReference.schoolYear'] == '')
    )]

    result_data_frame = subset(result_data_frame, [
        'id',
        'schoolReference.schoolId',
        'schoolYearTypeReference.schoolYear',
        'studentReference.studentUniqueId',
        'calendarEventDescriptor',
        'date',
        'AttendanceEvent.Absence',
        'AttendanceEvent.Present',
        '_calendar_dates',
        '_attendance_events_school',
        '_student_school_attendance_events'
    ])

    get_descriptor_code_value_from_uri(result_data_frame, 'calendarEventDescriptor')
    result_data_frame = result_data_frame[result_data_frame['calendarEventDescriptor'] == 'Instructional day']

    result_data_frame['date_now'] = date.today()
    result_data_frame['date_now'] = to_datetime_key(result_data_frame, 'date_now')
    result_data_frame = result_data_frame[result_data_frame['date'] <= result_data_frame['date_now']]

    # --- Section attendance

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

    student_attendance_events['eventDate'] = to_datetime_key(student_attendance_events, 'eventDate')

    student_attendance_events = get_descriptor_constant(student_attendance_events, 'attendanceEventCategoryDescriptor')

    attendance_events = (
        student_attendance_events[[
            'studentReference.studentUniqueId', 'sectionReference.schoolId', 'eventDate', 'attendanceEventCategoryDescriptor_constantName'
        ]]
    )

    attendance_events = get_descriptor_constant(attendance_events, 'behaviorDescriptor')
    attendance_events = crossTab(
        index=[
            attendance_events['studentReference.studentUniqueId'],
            attendance_events['sectionReference.schoolId'],
            attendance_events['eventDate']
        ],
        columns=attendance_events['attendanceEventCategoryDescriptor_constantName']).reset_index()

    addColumnIfNotExists(attendance_events, 'AttendanceEvent.Present', 0)
    addColumnIfNotExists(attendance_events, 'AttendanceEvent.Absence', 0)

    student_attendance_events['_attendance_events_section'] = '|'

    student_attendance_events = pdMerge(
        left=student_attendance_events,
        right=attendance_events,
        how='left',
        leftOn=['sectionReference.schoolId', 'studentReference.studentUniqueId', 'eventDate'],
        rightOn=['sectionReference.schoolId', 'studentReference.studentUniqueId', 'eventDate'],
        suffixLeft=None,
        suffixRight='_attendance_events_section'
    )

    result_data_frame['_student_section_attendance_events'] = '|'

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=student_attendance_events,
        how='left',
        leftOn=['schoolReference.schoolId', 'studentReference.studentUniqueId', 'date'],
        rightOn=['sectionReference.schoolId', 'studentReference.studentUniqueId', 'eventDate'],
        suffixLeft=None,
        suffixRight='_student_section_attendance_events'
    )

    replace_null(result_data_frame, 'sectionReference.schoolYear', '')
    result_data_frame['sectionReference.schoolYear'] = to_datetime_key(result_data_frame, 'sectionReference.schoolYear')

    result_data_frame = result_data_frame[(
        (result_data_frame['schoolYearTypeReference.schoolYear'] == '')
        | (result_data_frame['schoolYearTypeReference.schoolYear'] == result_data_frame['sectionReference.schoolYear'])
        | (result_data_frame['sectionReference.schoolYear'] == '')
    )]

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

    student_section_associations_normalize['_student_section_associations'] = '|'

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=student_section_associations_normalize,
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
        suffixRight='_student_section_associations'
    )

    # ---

    result_data_frame = subset(result_data_frame, [
        'schoolReference.schoolId',
        'studentReference.studentUniqueId',
        'date',
        'AttendanceEvent.Absence',
        'AttendanceEvent.Present',
        'AttendanceEvent.Absence_student_section_attendance_events',
        'AttendanceEvent.Present_student_section_attendance_events'
    ])

    result_data_frame = result_data_frame.groupby(
        [
            'schoolReference.schoolId',
            'studentReference.studentUniqueId',
            'date'
        ]).max().reset_index()

    result_data_frame = renameColumns(result_data_frame, {
        'schoolReference.schoolId': 'SchoolKey',
        'studentReference.studentUniqueId': 'StudentKey',
        'date': 'DateKey',
        'AttendanceEvent.Absence': 'ReportedAsAbsentFromSchool',
        'AttendanceEvent.Present': 'ReportedAsPresentAtSchool',
        'AttendanceEvent.Absence_student_section_attendance_events': 'ReportedAsAbsentFromHomeRoom',
        'AttendanceEvent.Present_student_section_attendance_events': 'ReportedAsPresentAtHomeRoom',
    })

    result_data_frame['StudentSchoolKey'] = result_data_frame['StudentKey'].astype(str) + '-' + result_data_frame['SchoolKey'].astype(str)

    result_data_frame["ReportedAsAbsentFromSchool"] = result_data_frame.apply(
        lambda r: 1 if r["ReportedAsAbsentFromSchool"] > 0 else 0, axis=1
    )

    result_data_frame["ReportedAsPresentAtSchool"] = result_data_frame.apply(
        lambda r: 1 if r["ReportedAsPresentAtSchool"] > 0 else 0, axis=1
    )

    result_data_frame["ReportedAsAbsentFromHomeRoom"] = result_data_frame.apply(
        lambda r: 1 if r["ReportedAsAbsentFromHomeRoom"] > 0 else 0, axis=1
    )

    result_data_frame["ReportedAsPresentAtHomeRoom"] = result_data_frame.apply(
        lambda r: 1 if r["ReportedAsPresentAtHomeRoom"] > 0 else 0, axis=1
    )

    result_data_frame["ReportedAsIsPresentInAllSections"] = result_data_frame.apply(
        lambda r: 1 if r["ReportedAsAbsentFromHomeRoom"] == 0 & r["ReportedAsAbsentFromHomeRoom"] == 1 else 0, axis=1
    )

    result_data_frame["ReportedAsAbsentFromAnySection"] = result_data_frame.apply(
        lambda r: 1 if r["ReportedAsAbsentFromHomeRoom"] == 1 else 0, axis=1
    )

    return result_data_frame[
        columns
    ]


def chronic_absenteeism_attendance_fact(school_year) -> None:
    return chronic_absenteeism_attendance_fact_dataframe(
        file_name="chrab_chronicAbsenteeismAttendanceFact.parquet",
        columns=RESULT_COLUMNS,
        school_year=school_year
    )

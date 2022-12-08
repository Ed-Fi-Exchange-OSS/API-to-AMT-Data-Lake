# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from datetime import date

from decouple import config

from edfi_amt_data_lake.helper.data_frame_generation_result import (
    data_frame_generation_result,
)
from edfi_amt_data_lake.parquet.Common.descriptor_mapping import get_descriptor_constant
from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    add_dataframe_column,
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

ENDPOINT_CALENDAR_DATES = 'calendarDates'
ENDPOINT_DISCIPLINE_INCIDENTS = 'disciplineIncidents'
ENDPOINT_STUDENT_DISCIPLINE_BEHAVIOR_ASSOCIATION = 'studentDisciplineIncidentBehaviorAssociations'
ENDPOINT_STUDENT_SCHOOL_ASSOCIATION = 'studentSchoolAssociations'
ENDPOINT_STUDENT_SECTION_ASSOCIATION = 'studentSectionAssociations'
ENDPOINT_STUDENT_SCHOOL_ATTENDANCE_EVENTS = 'studentSchoolAttendanceEvents'
ENDPOINT_STUDENT_SECTION_ATTENDANCE_EVENTS = 'studentSectionAttendanceEvents'
IS_INSTRUCTIONAL_DAY = 'CalendarEvent.InstructionalDay'
RESULT_COLUMNS = [
    'StudentKey',
    'SchoolKey',
    'DateKey',
    'IsInstructionalDay',
    'IsEnrolled',
    'IsPresentSchool',
    'IsAbsentFromSchoolExcused',
    'IsAbsentFromSchoolUnexcused',
    'IsTardyToSchool',
    'IsPresentHomeroom',
    'IsAbsentFromHomeroomExcused',
    'IsAbsentFromHomeroomUnexcused',
    'IsTardyToHomeroom',
    'IsPresentAnyClass',
    'IsAbsentFromAnyClassExcused',
    'IsAbsentFromAnyClassUnexcused',
    'IsTardyToAnyClass',
    'CountByDayOfStateOffenses',
    'CountByDayOfConductOffenses'
]


@create_parquet_file
def student_early_warning_fact_data_frame(
    file_name: str,
    columns: list[str],
    school_year: int
):
    file_name = file_name
    calendar_dates_content = getEndpointJson(ENDPOINT_CALENDAR_DATES, config('SILVER_DATA_LOCATION'), school_year)
    discipline_incident_content = getEndpointJson(ENDPOINT_DISCIPLINE_INCIDENTS, config('SILVER_DATA_LOCATION'), school_year)
    student_discipline_incident_behavior_associations_content = getEndpointJson(ENDPOINT_STUDENT_DISCIPLINE_BEHAVIOR_ASSOCIATION, config('SILVER_DATA_LOCATION'), school_year)
    student_school_associations_content = getEndpointJson(ENDPOINT_STUDENT_SCHOOL_ASSOCIATION, config('SILVER_DATA_LOCATION'), school_year)
    student_section_associations_content = getEndpointJson(ENDPOINT_STUDENT_SECTION_ASSOCIATION, config('SILVER_DATA_LOCATION'), school_year)
    student_school_attendance_events_content = getEndpointJson(ENDPOINT_STUDENT_SCHOOL_ATTENDANCE_EVENTS, config('SILVER_DATA_LOCATION'), school_year)
    student_section_attendance_events_content = getEndpointJson(ENDPOINT_STUDENT_SECTION_ATTENDANCE_EVENTS, config('SILVER_DATA_LOCATION'), school_year)

    ############################
    # studentSchoolAssociation
    ############################
    student_school_association_normalized = jsonNormalize(
        student_school_associations_content,
        recordPath=None,
        meta=[
            'schoolReference.schoolId',
            'studentReference.studentUniqueId',
            'entryDate',
            'exitWithdrawDate'
        ],
        metaPrefix=None,
        recordPrefix='calendarEvents_',
        errors='ignore'
    )

    # Select needed columns.
    student_school_association_normalized = subset(student_school_association_normalized, [
        'schoolReference.schoolId',
        'studentReference.studentUniqueId',
        'entryDate',
        'exitWithdrawDate'
    ])

    student_school_association_normalized = renameColumns(student_school_association_normalized, {
        'schoolReference.schoolId': 'schoolId',
        'studentReference.studentUniqueId': 'studentUniqueId'
    })

    ############################
    # Calendar dates
    ############################
    calendar_dates_normalized = jsonNormalize(
        calendar_dates_content,
        recordPath=['calendarEvents'],
        meta=[
            'date',
            ['calendarReference', 'schoolId'],
            ['calendarReference', 'schoolYear']
        ],
        recordMeta=[
            'calendarEventDescriptor'
        ],
        metaPrefix=None,
        recordPrefix='calendarEvents_',
        errors='ignore'
    )

    calendar_dates_normalized = get_descriptor_constant(calendar_dates_normalized, 'calendarEvents_calendarEventDescriptor')

    if not calendar_dates_normalized.empty:
        calendar_dates_normalized.loc[calendar_dates_normalized['calendarEvents_calendarEventDescriptor_constantName'] == IS_INSTRUCTIONAL_DAY, 'IsInstructionalDay'] = '1'
        calendar_dates_normalized.loc[calendar_dates_normalized['calendarEvents_calendarEventDescriptor_constantName'] != IS_INSTRUCTIONAL_DAY, 'IsInstructionalDay'] = '0'
    replace_null(calendar_dates_normalized, 'IsInstructionalDay', '0')
    # Select needed columns.
    calendar_dates_normalized = subset(calendar_dates_normalized, [
        'IsInstructionalDay',
        'calendarReference.schoolId',
        'calendarReference.schoolYear',
        'date'
    ])

    calendar_dates_normalized = renameColumns(calendar_dates_normalized, {
        'calendarReference.schoolId': 'schoolId',
        'calendarReference.schoolYear': 'schoolYear'
    })

    ############################
    # studentSchoolAssociations - calendarDates
    ############################
    result_data_frame = pdMerge(
        left=student_school_association_normalized,
        right=calendar_dates_normalized,
        how='inner',
        leftOn=['schoolId'],
        rightOn=['schoolId'],
        suffixLeft='_studentSchoolAssociation',
        suffixRight='_calendarDates'
    )
    if result_data_frame is None:
        return None
    result_data_frame['exitWithdrawDateKey'] = to_datetime_key(result_data_frame, 'exitWithdrawDate')
    result_data_frame['dateKey'] = to_datetime_key(result_data_frame, 'date')
    result_data_frame['entryDateKey'] = to_datetime_key(result_data_frame, 'entryDate')
    result_data_frame['date_now'] = date.today()
    result_data_frame['date_now'] = to_datetime_key(result_data_frame, 'date_now')
    result_data_frame = result_data_frame[result_data_frame['entryDateKey'] <= result_data_frame['dateKey']]
    result_data_frame = result_data_frame[result_data_frame['exitWithdrawDateKey'] >= result_data_frame['dateKey']]
    result_data_frame = result_data_frame[result_data_frame['dateKey'] <= result_data_frame['date_now']]

    ############################
    # StudentSchoolAttendance
    ############################
    student_school_attendance_events_normalized = jsonNormalize(
        student_school_attendance_events_content,
        recordPath=None,
        meta=['schoolReference.schoolId', 'studentReference.studentUniqueId', 'eventDate', 'attendanceEventCategoryDescriptor'],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    # Get descriptor code value
    student_school_attendance_events_normalized = get_descriptor_constant(student_school_attendance_events_normalized, 'attendanceEventCategoryDescriptor')
    # Select needed columns.
    student_school_attendance_events_normalized = subset(student_school_attendance_events_normalized, [
        'schoolReference.schoolId',
        'studentReference.studentUniqueId',
        'eventDate',
        'attendanceEventCategoryDescriptor_constantName'
    ])

    student_school_attendance_events_normalized = renameColumns(student_school_attendance_events_normalized, {
        'schoolReference.schoolId': 'schoolId',
        'studentReference.studentUniqueId': 'studentUniqueId'
    })
    # 'Transpose' Attendance table.
    student_school_attendance_events_normalized = crossTab(
        index=[
            student_school_attendance_events_normalized['schoolId'],
            student_school_attendance_events_normalized['studentUniqueId'],
            student_school_attendance_events_normalized['eventDate']
        ],
        columns=student_school_attendance_events_normalized['attendanceEventCategoryDescriptor_constantName']).reset_index()
    # Rename attendance columns
    student_school_attendance_events_normalized = renameColumns(student_school_attendance_events_normalized, {
        'AttendanceEvent.Present': 'IsPresentSchool',
        'AttendanceEvent.ExcusedAbsence': 'IsAbsentFromSchoolExcused',
        'AttendanceEvent.UnexcusedAbsence': 'IsAbsentFromSchoolUnexcused',
        'AttendanceEvent.Tardy': 'IsTardyToSchool'
    })

    ############################
    # Result - student_school_attendance_events_normalized
    ############################
    result_data_frame = pdMerge(
        left=result_data_frame,
        right=student_school_attendance_events_normalized,
        how='left',
        leftOn=['schoolId', 'studentUniqueId', 'date'],
        rightOn=['schoolId', 'studentUniqueId', 'eventDate'],
        suffixLeft='_studentSchoolAssociation',
        suffixRight='_studentSchoolAttendanceEvents'
    )
    if result_data_frame is None:
        return None
    ####################################################################################
    # By Section
    ####################################################################################

    ############################
    # student_section_associations_normalized
    ############################
    student_section_associations_normalized = jsonNormalize(
        student_section_associations_content,
        recordPath=None,
        meta=[
            'sectionReference.localCourseCode',
            'sectionReference.schoolId',
            'sectionReference.schoolYear',
            'sectionReference.sectionIdentifier',
            'sectionReference.sessionName',
            'studentReference.studentUniqueId',
            'endDate',
            'homeroomIndicator'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    # Select needed columns.
    student_section_associations_normalized = subset(student_section_associations_normalized, [
        'sectionReference.localCourseCode',
        'sectionReference.schoolId',
        'sectionReference.schoolYear',
        'sectionReference.sectionIdentifier',
        'sectionReference.sessionName',
        'studentReference.studentUniqueId',
        'endDate',
        'homeroomIndicator'
    ])

    student_section_associations_normalized = renameColumns(student_section_associations_normalized, {
        'sectionReference.localCourseCode' : 'localCourseCode',
        'sectionReference.schoolId' : 'schoolId',
        'sectionReference.schoolYear' : 'schoolYear',
        'sectionReference.sectionIdentifier' : 'sectionIdentifier',
        'sectionReference.sessionName' : 'sessionName',
        'studentReference.studentUniqueId' : 'studentUniqueId'
    })

    ############################
    # StudentSectionAttendance
    ############################
    student_section_attendance_events_normalized = jsonNormalize(
        student_section_attendance_events_content,
        recordPath=None,
        meta=[
            'sectionReference.localCourseCode',
            'sectionReference.schoolId',
            'sectionReference.schoolYear',
            'sectionReference.sectionIdentifier',
            'sectionReference.sessionName',
            'studentReference.studentUniqueId',
            'eventDate',
            'attendanceEventCategoryDescriptor',
            'educationalEnvironmentDescriptor'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    # Get descriptor code value
    get_descriptor_code_value_from_uri(student_section_attendance_events_normalized, 'attendanceEventCategoryDescriptor')

    # Select needed columns.
    student_section_attendance_events_normalized = subset(student_section_attendance_events_normalized, [
        'sectionReference.localCourseCode',
        'sectionReference.schoolId',
        'sectionReference.schoolYear',
        'sectionReference.sectionIdentifier',
        'sectionReference.sessionName',
        'studentReference.studentUniqueId',
        'eventDate',
        'attendanceEventCategoryDescriptor',
        'educationalEnvironmentDescriptor'
    ])

    student_section_attendance_events_normalized = renameColumns(student_section_attendance_events_normalized, {
        'sectionReference.localCourseCode' : 'localCourseCode',
        'sectionReference.schoolId' : 'schoolId',
        'sectionReference.schoolYear' : 'schoolYear',
        'sectionReference.sectionIdentifier' : 'sectionIdentifier',
        'sectionReference.sessionName' : 'sessionName',
        'studentReference.studentUniqueId' : 'studentUniqueId'
    })

    student_section_attendance_events_normalized = crossTab(
        index=[
            student_section_attendance_events_normalized['localCourseCode'],
            student_section_attendance_events_normalized['schoolId'],
            student_section_attendance_events_normalized['schoolYear'],
            student_section_attendance_events_normalized['sectionIdentifier'],
            student_section_attendance_events_normalized['sessionName'],
            student_section_attendance_events_normalized['studentUniqueId'],
            student_section_attendance_events_normalized['eventDate'],
            student_section_attendance_events_normalized['educationalEnvironmentDescriptor']
        ],
        columns=student_section_attendance_events_normalized['attendanceEventCategoryDescriptor']).reset_index()

    student_section_attendance_events_normalized = renameColumns(student_section_attendance_events_normalized, {
        'In Attendance': 'IsPresentAnyClass',
        'Excused Absence': 'IsAbsentFromAnyClassExcused',
        'Unexcused Absence': 'IsAbsentFromAnyClassUnexcused',
        'Tardy': 'IsTardyToAnyClass'
    }).reset_index()

    addColumnIfNotExists(student_section_attendance_events_normalized, 'IsPresentAnyClass', 0)
    addColumnIfNotExists(student_section_attendance_events_normalized, 'IsAbsentFromAnyClassExcused', 0)
    addColumnIfNotExists(student_section_attendance_events_normalized, 'IsAbsentFromAnyClassUnexcused', 0)
    addColumnIfNotExists(student_section_attendance_events_normalized, 'IsTardyToAnyClass', 0)
    student_section_attendance_events_normalized.loc[student_section_attendance_events_normalized['IsPresentAnyClass'] == '', 'IsPresentAnyClass'] = '0'
    student_section_attendance_events_normalized.loc[student_section_attendance_events_normalized['IsAbsentFromAnyClassExcused'] == '', 'IsAbsentFromAnyClassExcused'] = '0'
    student_section_attendance_events_normalized.loc[student_section_attendance_events_normalized['IsAbsentFromAnyClassUnexcused'] == '', 'IsAbsentFromAnyClassUnexcused'] = '0'
    student_section_attendance_events_normalized.loc[student_section_attendance_events_normalized['IsTardyToAnyClass'] == '', 'IsTardyToAnyClass'] = '0'

    student_section_attendance_events_normalized = subset(student_section_attendance_events_normalized, [
        'localCourseCode',
        'schoolId',
        'schoolYear',
        'sectionIdentifier',
        'sessionName',
        'studentUniqueId',
        'eventDate',
        'IsPresentAnyClass',
        'IsAbsentFromAnyClassExcused',
        'IsAbsentFromAnyClassUnexcused',
        'IsTardyToAnyClass'
    ])

    ############################
    # StudentSectionAssociation - SectionAttendance
    ############################
    student_section_attendance_events_normalized = pdMerge(
        left=student_section_associations_normalized,
        right=student_section_attendance_events_normalized,
        how='inner',
        leftOn=[
            'localCourseCode',
            'schoolId',
            'schoolYear',
            'sectionIdentifier',
            'sessionName',
            'studentUniqueId'
        ],
        rightOn=[
            'localCourseCode',
            'schoolId',
            'schoolYear',
            'sectionIdentifier',
            'sessionName',
            'studentUniqueId'
        ],
        suffixLeft='_studentSchool',
        suffixRight='_studentSection'
    )

    ############################
    # Subset homeroom values
    ############################
    student_section_homeroom_dataFrame = student_section_attendance_events_normalized[student_section_attendance_events_normalized['homeroomIndicator']]
    student_section_homeroom_dataFrame = add_dataframe_column(
        student_section_homeroom_dataFrame,
        [
            'localCourseCode',
            'schoolId',
            'schoolYear',
            'sectionIdentifier',
            'sessionName',
            'studentUniqueId',
            'eventDate',
            'endDate',
            'homeroomIndicator',
            'IsPresentHomeroom',
            'IsAbsentFromHomeroomExcused',
            'IsAbsentFromHomeroomUnexcused',
            'IsTardyToHomeroom'
        ]
    )
    student_section_homeroom_dataFrame = renameColumns(student_section_homeroom_dataFrame, {
        'IsPresentAnyClass': 'IsPresentHomeroom',
        'IsAbsentFromAnyClassExcused': 'IsAbsentFromHomeroomExcused',
        'IsAbsentFromAnyClassUnexcused': 'IsAbsentFromHomeroomUnexcused',
        'IsTardyToAnyClass': 'IsTardyToHomeroom'
    })
    ############################
    # Add Homeroom columns
    ############################
    student_section_attendance_events_normalized = pdMerge(
        left=student_section_attendance_events_normalized,
        right=student_section_homeroom_dataFrame,
        how='left',
        leftOn=[
            'localCourseCode',
            'schoolId',
            'schoolYear',
            'sectionIdentifier',
            'sessionName',
            'studentUniqueId',
            'eventDate',
            'endDate',
            'homeroomIndicator'],
        rightOn=[
            'localCourseCode',
            'schoolId',
            'schoolYear',
            'sectionIdentifier',
            'sessionName',
            'studentUniqueId',
            'eventDate',
            'endDate',
            'homeroomIndicator'],
        suffixLeft='_studentSectionAssociationAttendance',
        suffixRight='_studentSectionAssociationAttendanceHomeroom'
    )

    student_section_attendance_events_normalized = subset(student_section_attendance_events_normalized, [
        'schoolId',
        'schoolYear',
        'studentUniqueId',
        'eventDate',
        'IsPresentAnyClass',
        'IsAbsentFromAnyClassExcused',
        'IsAbsentFromAnyClassUnexcused',
        'IsTardyToAnyClass',
        'IsPresentHomeroom',
        'IsAbsentFromHomeroomExcused',
        'IsAbsentFromHomeroomUnexcused',
        'IsTardyToHomeroom'
    ])

    student_section_attendance_events_normalized = student_section_attendance_events_normalized.groupby(
        [
            'schoolId',
            'schoolYear',
            'studentUniqueId',
            'eventDate'
        ]).max().reset_index()

    ############################
    # Result - StudentSectionAttendance
    ############################
    result_data_frame = pdMerge(
        left=result_data_frame,
        right=student_section_attendance_events_normalized,
        how='left',
        leftOn=[
            'schoolId',
            'studentUniqueId',
            'date',
            'schoolYear'
        ],
        rightOn=[
            'schoolId',
            'studentUniqueId',
            'eventDate',
            'schoolYear'
        ],
        suffixLeft='_result',
        suffixRight='_studentSectionAssociationAttendance'
    )

    ############################
    # Discipline Incident
    ############################
    discipline_incident_normalized = jsonNormalize(
        discipline_incident_content,
        recordPath=None,
        meta=[
            'schoolReference.schoolId',
            'incidentIdentifier',
            'incidentDate'
        ],
        metaPrefix=None,
        recordPrefix='',
        errors='ignore'
    )

    # Select needed columns.
    discipline_incident_normalized = subset(discipline_incident_normalized, [
        'schoolReference.schoolId',
        'incidentIdentifier',
        'incidentDate'
    ])

    discipline_incident_normalized = renameColumns(discipline_incident_normalized, {
        'schoolReference.schoolId': 'schoolId'
    })

    ############################
    # Student Discipline Behavior
    ############################
    student_discipline_incident_behavior_associations_normalized = jsonNormalize(
        student_discipline_incident_behavior_associations_content,
        recordPath=None,
        meta=[
            'disciplineIncidentReference.incidentIdentifier',
            'disciplineIncidentReference.schoolId',
            'studentReference.studentUniqueId',
            'behaviorDescriptor'
        ],
        metaPrefix=None,
        recordPrefix='calendarEvents_',
        errors='ignore'
    )

    # Select needed columns.
    student_discipline_incident_behavior_associations_normalized = subset(student_discipline_incident_behavior_associations_normalized, [
        'disciplineIncidentReference.incidentIdentifier',
        'disciplineIncidentReference.schoolId',
        'studentReference.studentUniqueId',
        'behaviorDescriptor'
    ])

    student_discipline_incident_behavior_associations_normalized = renameColumns(student_discipline_incident_behavior_associations_normalized, {
        'disciplineIncidentReference.incidentIdentifier': 'incidentIdentifier',
        'disciplineIncidentReference.schoolId': 'schoolId',
        'studentReference.studentUniqueId': 'studentUniqueId'
    })

    result_discipline_dataFrame = pdMerge(
        left=discipline_incident_normalized,
        right=student_discipline_incident_behavior_associations_normalized,
        how='inner',
        leftOn=[
            'schoolId',
            'incidentIdentifier'
        ],
        rightOn=[
            'schoolId',
            'incidentIdentifier'
        ],
        suffixLeft='_discipline',
        suffixRight='_disciplineBehavior'
    )

    result_discipline_dataFrame = get_descriptor_constant(result_discipline_dataFrame, 'behaviorDescriptor')
    result_discipline_dataFrame = crossTab(
        index=[
            result_discipline_dataFrame['schoolId'],
            result_discipline_dataFrame['studentUniqueId'],
            result_discipline_dataFrame['incidentDate']
        ],
        columns=result_discipline_dataFrame['behaviorDescriptor_constantName']).reset_index()

    # Select needed columns.
    result_discipline_dataFrame = renameColumns(result_discipline_dataFrame, {
        'Behavior.StateOffense': 'CountByDayOfStateOffenses',
        'Behavior.SchoolCodeOfConductOffense': 'CountByDayOfConductOffenses'
    })

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=result_discipline_dataFrame,
        how='left',
        leftOn=[
            'schoolId',
            'studentUniqueId',
            'date'],
        rightOn=[
            'schoolId',
            'studentUniqueId',
            'incidentDate'],
        suffixLeft='_attendanceDataFrame',
        suffixRight='_disciplineDataFrame'
    )

    result_data_frame['studentUniqueId'] = result_data_frame['studentUniqueId'].astype(str)
    result_data_frame['DateKey'] = to_datetime_key(result_data_frame, 'date')
    result_data_frame['IsEnrolled'] = 1

    addColumnIfNotExists(result_data_frame, 'IsPresentSchool', 0)
    addColumnIfNotExists(result_data_frame, 'IsAbsentFromSchoolExcused', 0)
    addColumnIfNotExists(result_data_frame, 'IsAbsentFromSchoolUnexcused', 0)
    addColumnIfNotExists(result_data_frame, 'IsTardyToSchool', 0)

    addColumnIfNotExists(result_data_frame, 'IsPresentHomeroom', 0)
    addColumnIfNotExists(result_data_frame, 'IsAbsentFromHomeroomExcused', 0)
    addColumnIfNotExists(result_data_frame, 'IsAbsentFromHomeroomUnexcused', 0)
    addColumnIfNotExists(result_data_frame, 'IsTardyToHomeroom', 0)

    addColumnIfNotExists(result_data_frame, 'CountByDayOfStateOffenses', 0)
    addColumnIfNotExists(result_data_frame, 'CountByDayOfConductOffenses', 0)

    # Replace null values by 0
    replace_null(result_data_frame, 'IsPresentSchool', 0)
    replace_null(result_data_frame, 'IsAbsentFromSchoolExcused', 0)
    replace_null(result_data_frame, 'IsAbsentFromSchoolUnexcused', 0)
    replace_null(result_data_frame, 'IsTardyToSchool', 0)
    replace_null(result_data_frame, 'IsPresentAnyClass', 0)
    replace_null(result_data_frame, 'IsAbsentFromAnyClassExcused', 0)
    replace_null(result_data_frame, 'IsAbsentFromAnyClassUnexcused', 0)
    replace_null(result_data_frame, 'IsTardyToAnyClass', 0)
    replace_null(result_data_frame, 'IsPresentHomeroom', 0)
    replace_null(result_data_frame, 'IsAbsentFromHomeroomExcused', 0)
    replace_null(result_data_frame, 'IsAbsentFromHomeroomUnexcused', 0)
    replace_null(result_data_frame, 'IsTardyToHomeroom', 0)
    replace_null(result_data_frame, 'CountByDayOfStateOffenses', 0)
    replace_null(result_data_frame, 'CountByDayOfConductOffenses', 0)

    result_data_frame = renameColumns(result_data_frame, {
        'studentUniqueId': 'StudentKey',
        'schoolId': 'SchoolKey'
    })

    # Select needed columns.
    result_data_frame = subset(result_data_frame, columns)
    return result_data_frame


def student_early_warning_fact(school_year) -> data_frame_generation_result:
    return student_early_warning_fact_data_frame(
        file_name="ews_StudentEarlyWarningFactDim.parquet",
        columns=RESULT_COLUMNS,
        school_year=school_year
    )

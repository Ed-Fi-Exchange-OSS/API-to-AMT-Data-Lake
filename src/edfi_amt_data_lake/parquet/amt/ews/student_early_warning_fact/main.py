# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from datetime import date

from decouple import config

from edfi_amt_data_lake.parquet.Common.descriptor_mapping import get_descriptor_constant
from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    addColumnIfNotExists,
    crossTab,
    get_descriptor_code_value_from_uri,
    jsonNormalize,
    pdMerge,
    renameColumns,
    replace_null,
    saveParquetFile,
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


def student_early_warning_fact(school_year) -> None:
    calendarDatesContent = getEndpointJson(ENDPOINT_CALENDAR_DATES, config('SILVER_DATA_LOCATION'), school_year)
    disciplineIncidentContent = getEndpointJson(ENDPOINT_DISCIPLINE_INCIDENTS, config('SILVER_DATA_LOCATION'), school_year)
    studentDisciplineIncidentBehaviorAssociationsContent = getEndpointJson(ENDPOINT_STUDENT_DISCIPLINE_BEHAVIOR_ASSOCIATION, config('SILVER_DATA_LOCATION'), school_year)
    studentSchoolAssociationsContent = getEndpointJson(ENDPOINT_STUDENT_SCHOOL_ASSOCIATION, config('SILVER_DATA_LOCATION'), school_year)
    studentSectionAssociationsContent = getEndpointJson(ENDPOINT_STUDENT_SECTION_ASSOCIATION, config('SILVER_DATA_LOCATION'), school_year)
    studentSchoolAttendanceEventsContent = getEndpointJson(ENDPOINT_STUDENT_SCHOOL_ATTENDANCE_EVENTS, config('SILVER_DATA_LOCATION'), school_year)
    studentSectionAttendanceEventsContent = getEndpointJson(ENDPOINT_STUDENT_SECTION_ATTENDANCE_EVENTS, config('SILVER_DATA_LOCATION'), school_year)

    ############################
    # studentSchoolAssociation
    ############################
    studentSchoolAssociationNormalized = jsonNormalize(
        studentSchoolAssociationsContent,
        recordPath=None,
        meta=['schoolReference.schoolId', 'studentReference.studentUniqueId', 'entryDate', 'exitWithdrawDate'],
        metaPrefix=None,
        recordPrefix='calendarEvents_',
        errors='ignore'
    )

    # Select needed columns.
    studentSchoolAssociationNormalized = subset(studentSchoolAssociationNormalized, [
        'schoolReference.schoolId',
        'studentReference.studentUniqueId',
        'entryDate',
        'exitWithdrawDate'
    ])

    studentSchoolAssociationNormalized = renameColumns(studentSchoolAssociationNormalized, {
        'schoolReference.schoolId': 'schoolId',
        'studentReference.studentUniqueId': 'studentUniqueId'
    })

    ############################
    # Calendar dates
    ############################
    calendarDatesNormalized = jsonNormalize(
        calendarDatesContent,
        recordPath=['calendarEvents'],
        meta=['date', ['calendarReference', 'schoolId'], ['calendarReference', 'schoolYear']],
        metaPrefix=None,
        recordPrefix='calendarEvents_',
        errors='ignore'
    )

    calendarDatesNormalized = get_descriptor_constant(calendarDatesNormalized, 'calendarEvents_calendarEventDescriptor')
    calendarDatesNormalized.loc[calendarDatesNormalized['calendarEvents_calendarEventDescriptor_constantName'] == IS_INSTRUCTIONAL_DAY, 'IsInstructionalDay'] = '1'
    calendarDatesNormalized.loc[calendarDatesNormalized['calendarEvents_calendarEventDescriptor_constantName'] != IS_INSTRUCTIONAL_DAY, 'IsInstructionalDay'] = '0'

    # Select needed columns.
    calendarDatesNormalized = subset(calendarDatesNormalized, [
        'IsInstructionalDay',
        'calendarReference.schoolId',
        'calendarReference.schoolYear',
        'date'
    ])

    calendarDatesNormalized = renameColumns(calendarDatesNormalized, {
        'calendarReference.schoolId': 'schoolId',
        'calendarReference.schoolYear': 'schoolYear'
    })

    ############################
    # studentSchoolAssociations - calendarDates
    ############################
    resultDataFrame = pdMerge(
        left=studentSchoolAssociationNormalized,
        right=calendarDatesNormalized,
        how='inner',
        leftOn=['schoolId'],
        rightOn=['schoolId'],
        suffixLeft='_studentSchoolAssociation',
        suffixRight='_calendarDates'
    )

    resultDataFrame['exitWithdrawDateKey'] = to_datetime_key(resultDataFrame, 'exitWithdrawDate')
    resultDataFrame['dateKey'] = to_datetime_key(resultDataFrame, 'date')
    resultDataFrame['entryDateKey'] = to_datetime_key(resultDataFrame, 'entryDate')
    resultDataFrame['date_now'] = date.today()
    resultDataFrame['date_now'] = to_datetime_key(resultDataFrame, 'date_now')
    resultDataFrame = resultDataFrame[resultDataFrame['entryDateKey'] <= resultDataFrame['dateKey']]
    resultDataFrame = resultDataFrame[resultDataFrame['exitWithdrawDateKey'] >= resultDataFrame['dateKey']]
    resultDataFrame = resultDataFrame[resultDataFrame['dateKey'] <= resultDataFrame['date_now']]

    ############################
    # StudentSchoolAttendance
    ############################
    studentSchoolAttendanceEventsNormalized = jsonNormalize(
        studentSchoolAttendanceEventsContent,
        recordPath=None,
        meta=['schoolReference.schoolId', 'studentReference.studentUniqueId', 'eventDate', 'attendanceEventCategoryDescriptor'],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    # Get descriptor code value
    studentSchoolAttendanceEventsNormalized = get_descriptor_constant(studentSchoolAttendanceEventsNormalized, 'attendanceEventCategoryDescriptor')
    # Select needed columns.
    studentSchoolAttendanceEventsNormalized = subset(studentSchoolAttendanceEventsNormalized, [
        'schoolReference.schoolId',
        'studentReference.studentUniqueId',
        'eventDate',
        'attendanceEventCategoryDescriptor_constantName'
    ])

    studentSchoolAttendanceEventsNormalized = renameColumns(studentSchoolAttendanceEventsNormalized, {
        'schoolReference.schoolId': 'schoolId',
        'studentReference.studentUniqueId': 'studentUniqueId'
    })
    # 'Transpose' Attendance table.
    studentSchoolAttendanceEventsNormalized = crossTab(
        index=[
            studentSchoolAttendanceEventsNormalized['schoolId'],
            studentSchoolAttendanceEventsNormalized['studentUniqueId'],
            studentSchoolAttendanceEventsNormalized['eventDate']
        ],
        columns=studentSchoolAttendanceEventsNormalized['attendanceEventCategoryDescriptor_constantName']).reset_index()
    # Rename attendance columns
    studentSchoolAttendanceEventsNormalized = renameColumns(studentSchoolAttendanceEventsNormalized, {
        'AttendanceEvent.Present': 'IsPresentSchool',
        'AttendanceEvent.ExcusedAbsence': 'IsAbsentFromSchoolExcused',
        'AttendanceEvent.UnexcusedAbsence': 'IsAbsentFromSchoolUnexcused',
        'AttendanceEvent.Tardy': 'IsTardyToSchool'
    })

    ############################
    # Result - studentSchoolAttendanceEventsNormalized
    ############################
    resultDataFrame = pdMerge(
        left=resultDataFrame,
        right=studentSchoolAttendanceEventsNormalized,
        how='left',
        leftOn=['schoolId', 'studentUniqueId', 'date'],
        rightOn=['schoolId', 'studentUniqueId', 'eventDate'],
        suffixLeft='_studentSchoolAssociation',
        suffixRight='_studentSchoolAttendanceEvents'
    )

    ####################################################################################
    # By Section
    ####################################################################################

    ############################
    # studentSectionAssociationsNormalized
    ############################
    studentSectionAssociationsNormalized = jsonNormalize(
        studentSectionAssociationsContent,
        recordPath=None,
        meta=[
            'sectionReference.localCourseCode',
            'sectionReference.schoolId',
            'sectionReference.schoolYear',
            'sectionReference.sectionIdentifier',
            'sectionReference.sessionName',
            'studentReference.studentUniqueId',
            'endDate',
            'homeroomIndicator'],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    # Select needed columns.
    studentSectionAssociationsNormalized = subset(studentSectionAssociationsNormalized, [
        'sectionReference.localCourseCode',
        'sectionReference.schoolId',
        'sectionReference.schoolYear',
        'sectionReference.sectionIdentifier',
        'sectionReference.sessionName',
        'studentReference.studentUniqueId',
        'endDate',
        'homeroomIndicator'
    ])

    studentSectionAssociationsNormalized = renameColumns(studentSectionAssociationsNormalized, {
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
    studentSectionAttendanceEventsNormalized = jsonNormalize(
        studentSectionAttendanceEventsContent,
        recordPath=None,
        meta=[
            'sectionReference.localCourseCode',
            'sectionReference.schoolId',
            'sectionReference.schoolYear',
            'sectionReference.sectionIdentifier',
            'sectionReference.sessionName',
            'studentReference.studentUniqueId',
            'eventDate',
            'attendanceEventCategoryDescriptor'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    # Get descriptor code value
    get_descriptor_code_value_from_uri(studentSectionAttendanceEventsNormalized, 'attendanceEventCategoryDescriptor')

    # Select needed columns.
    studentSectionAttendanceEventsNormalized = subset(studentSectionAttendanceEventsNormalized, [
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

    studentSectionAttendanceEventsNormalized = renameColumns(studentSectionAttendanceEventsNormalized, {
        'sectionReference.localCourseCode' : 'localCourseCode',
        'sectionReference.schoolId' : 'schoolId',
        'sectionReference.schoolYear' : 'schoolYear',
        'sectionReference.sectionIdentifier' : 'sectionIdentifier',
        'sectionReference.sessionName' : 'sessionName',
        'studentReference.studentUniqueId' : 'studentUniqueId'
    })

    studentSectionAttendanceEventsNormalized = crossTab(
        index=[
            studentSectionAttendanceEventsNormalized['localCourseCode'],
            studentSectionAttendanceEventsNormalized['schoolId'],
            studentSectionAttendanceEventsNormalized['schoolYear'],
            studentSectionAttendanceEventsNormalized['sectionIdentifier'],
            studentSectionAttendanceEventsNormalized['sessionName'],
            studentSectionAttendanceEventsNormalized['studentUniqueId'],
            studentSectionAttendanceEventsNormalized['eventDate'],
            studentSectionAttendanceEventsNormalized['educationalEnvironmentDescriptor']
        ],
        columns=studentSectionAttendanceEventsNormalized['attendanceEventCategoryDescriptor']).reset_index()

    studentSectionAttendanceEventsNormalized = renameColumns(studentSectionAttendanceEventsNormalized, {
        'In Attendance': 'IsPresentAnyClass',
        'Excused Absence': 'IsAbsentFromAnyClassExcused',
        'Unexcused Absence': 'IsAbsentFromAnyClassUnexcused',
        'Tardy': 'IsTardyToAnyClass'
    }).reset_index()

    addColumnIfNotExists(studentSectionAttendanceEventsNormalized, 'IsPresentAnyClass', 0)
    addColumnIfNotExists(studentSectionAttendanceEventsNormalized, 'IsAbsentFromAnyClassExcused', 0)
    addColumnIfNotExists(studentSectionAttendanceEventsNormalized, 'IsAbsentFromAnyClassUnexcused', 0)
    addColumnIfNotExists(studentSectionAttendanceEventsNormalized, 'IsTardyToAnyClass', 0)
    studentSectionAttendanceEventsNormalized.loc[studentSectionAttendanceEventsNormalized['IsPresentAnyClass'] == '', 'IsPresentAnyClass'] = '0'
    studentSectionAttendanceEventsNormalized.loc[studentSectionAttendanceEventsNormalized['IsAbsentFromAnyClassExcused'] == '', 'IsAbsentFromAnyClassExcused'] = '0'
    studentSectionAttendanceEventsNormalized.loc[studentSectionAttendanceEventsNormalized['IsAbsentFromAnyClassUnexcused'] == '', 'IsAbsentFromAnyClassUnexcused'] = '0'
    studentSectionAttendanceEventsNormalized.loc[studentSectionAttendanceEventsNormalized['IsTardyToAnyClass'] == '', 'IsTardyToAnyClass'] = '0'

    studentSectionAttendanceEventsNormalized = subset(studentSectionAttendanceEventsNormalized, [
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
    studentSectionAttendanceEventsNormalized = pdMerge(
        left=studentSectionAssociationsNormalized,
        right=studentSectionAttendanceEventsNormalized,
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
    studentSectionHomeroomDataFrame = studentSectionAttendanceEventsNormalized[studentSectionAttendanceEventsNormalized['homeroomIndicator']]

    studentSectionHomeroomDataFrame = renameColumns(studentSectionHomeroomDataFrame, {
        'IsPresentAnyClass': 'IsPresentHomeroom',
        'IsAbsentFromAnyClassExcused': 'IsAbsentFromHomeroomExcused',
        'IsAbsentFromAnyClassUnexcused': 'IsAbsentFromHomeroomUnexcused',
        'IsTardyToAnyClass': 'IsTardyToHomeroom'
    })

    ############################
    # Add Homeroom columns
    ############################
    studentSectionAttendanceEventsNormalized = pdMerge(
        left=studentSectionAttendanceEventsNormalized,
        right=studentSectionHomeroomDataFrame,
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

    studentSectionAttendanceEventsNormalized = subset(studentSectionAttendanceEventsNormalized, [
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

    studentSectionAttendanceEventsNormalized = studentSectionAttendanceEventsNormalized.groupby(
        [
            'schoolId',
            'schoolYear',
            'studentUniqueId',
            'eventDate'
        ]).max().reset_index()

    ############################
    # Result - StudentSectionAttendance
    ############################
    resultDataFrame = pdMerge(
        left=resultDataFrame,
        right=studentSectionAttendanceEventsNormalized,
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
    disciplineIncidentNormalized = jsonNormalize(
        disciplineIncidentContent,
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
    disciplineIncidentNormalized = subset(disciplineIncidentNormalized, [
        'schoolReference.schoolId',
        'incidentIdentifier',
        'incidentDate'
    ])

    disciplineIncidentNormalized = renameColumns(disciplineIncidentNormalized, {
        'schoolReference.schoolId': 'schoolId'
    })

    ############################
    # Student Discipline Behavior
    ############################
    studentDisciplineIncidentBehaviorAssociationsNormalized = jsonNormalize(
        studentDisciplineIncidentBehaviorAssociationsContent,
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
    studentDisciplineIncidentBehaviorAssociationsNormalized = subset(studentDisciplineIncidentBehaviorAssociationsNormalized, [
        'disciplineIncidentReference.incidentIdentifier',
        'disciplineIncidentReference.schoolId',
        'studentReference.studentUniqueId',
        'behaviorDescriptor'
    ])

    studentDisciplineIncidentBehaviorAssociationsNormalized = renameColumns(studentDisciplineIncidentBehaviorAssociationsNormalized, {
        'disciplineIncidentReference.incidentIdentifier': 'incidentIdentifier',
        'disciplineIncidentReference.schoolId': 'schoolId',
        'studentReference.studentUniqueId': 'studentUniqueId'
    })

    resultDisciplineDataFrame = pdMerge(
        left=disciplineIncidentNormalized,
        right=studentDisciplineIncidentBehaviorAssociationsNormalized,
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

    resultDisciplineDataFrame = get_descriptor_constant(resultDisciplineDataFrame, 'behaviorDescriptor')
    resultDisciplineDataFrame = crossTab(
        index=[
            resultDisciplineDataFrame['schoolId'],
            resultDisciplineDataFrame['studentUniqueId'],
            resultDisciplineDataFrame['incidentDate']
        ],
        columns=resultDisciplineDataFrame['behaviorDescriptor_constantName']).reset_index()

    # Select needed columns.
    resultDisciplineDataFrame = renameColumns(resultDisciplineDataFrame, {
        'Behavior.StateOffense': 'CountByDayOfStateOffenses',
        'Behavior.SchoolCodeOfConductOffense': 'CountByDayOfConductOffenses'
    })

    resultDataFrame = pdMerge(
        left=resultDataFrame,
        right=resultDisciplineDataFrame,
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

    resultDataFrame['studentUniqueId'] = resultDataFrame['studentUniqueId'].astype(str)
    resultDataFrame['DateKey'] = to_datetime_key(resultDataFrame, 'date')
    resultDataFrame['IsEnrolled'] = 1

    addColumnIfNotExists(resultDataFrame, 'IsPresentSchool', 0)
    addColumnIfNotExists(resultDataFrame, 'IsAbsentFromSchoolExcused', 0)
    addColumnIfNotExists(resultDataFrame, 'IsAbsentFromSchoolUnexcused', 0)
    addColumnIfNotExists(resultDataFrame, 'IsTardyToSchool', 0)

    addColumnIfNotExists(resultDataFrame, 'IsPresentHomeroom', 0)
    addColumnIfNotExists(resultDataFrame, 'IsAbsentFromHomeroomExcused', 0)
    addColumnIfNotExists(resultDataFrame, 'IsAbsentFromHomeroomUnexcused', 0)
    addColumnIfNotExists(resultDataFrame, 'IsTardyToHomeroom', 0)

    addColumnIfNotExists(resultDataFrame, 'CountByDayOfStateOffenses', 0)
    addColumnIfNotExists(resultDataFrame, 'CountByDayOfConductOffenses', 0)

    # Replace null values by 0
    replace_null(resultDataFrame, 'IsPresentSchool', 0)
    replace_null(resultDataFrame, 'IsAbsentFromSchoolExcused', 0)
    replace_null(resultDataFrame, 'IsAbsentFromSchoolUnexcused', 0)
    replace_null(resultDataFrame, 'IsTardyToSchool', 0)
    replace_null(resultDataFrame, 'IsPresentAnyClass', 0)
    replace_null(resultDataFrame, 'IsAbsentFromAnyClassExcused', 0)
    replace_null(resultDataFrame, 'IsAbsentFromAnyClassUnexcused', 0)
    replace_null(resultDataFrame, 'IsTardyToAnyClass', 0)
    replace_null(resultDataFrame, 'IsPresentHomeroom', 0)
    replace_null(resultDataFrame, 'IsAbsentFromHomeroomExcused', 0)
    replace_null(resultDataFrame, 'IsAbsentFromHomeroomUnexcused', 0)
    replace_null(resultDataFrame, 'IsTardyToHomeroom', 0)
    replace_null(resultDataFrame, 'CountByDayOfStateOffenses', 0)
    replace_null(resultDataFrame, 'CountByDayOfConductOffenses', 0)

    resultDataFrame = renameColumns(resultDataFrame, {
        'studentUniqueId': 'StudentKey',
        'schoolId': 'SchoolKey'
    })

    # Select needed columns.
    resultDataFrame = subset(resultDataFrame, [
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
    ])

    saveParquetFile(resultDataFrame, f"{config('PARQUET_FILES_LOCATION')}", "ews_StudentEarlyWarningFactDim.parquet", school_year)

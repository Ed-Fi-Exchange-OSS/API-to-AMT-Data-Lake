# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

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
    subset,
    to_datetime_key,
)

ENDPOINT_ACADEMIC_SUBJECT_DESCRIPTOR = 'academicSubjectDescriptors'
ENDPOINT_STUDENT_SECTION_ASSOCIATIONS = 'studentSectionAssociations'
ENDPOINT_STAFF = 'staffs'
ENDPOINT_STAFF_SECTION_ASSOCIATIONS = 'staffSectionAssociations'
ENDPOINT_COURSE = 'courses'
ENDPOINT_COURSE_OFFERING = 'courseOfferings'
ENDPOINT_SECTIONS = 'sections'
ENDPOINT_SESSIONS = 'sessions'
RESULT_COLUMNS = [
    'StudentSectionKey',
    'StudentSchoolKey',
    'StudentKey',
    'SectionKey',
    'LocalCourseCode',
    'Subject',
    'CourseTitle',
    'TeacherName',
    'StudentSectionStartDateKey',
    'StudentSectionEndDateKey',
    'SchoolKey',
    'SchoolYear',
]


@create_parquet_file
def section_dim_dataframe(
    file_name: str,
    columns: list[str],
    school_year: int
) -> pd.DataFrame:
    academic_subject_descriptor_content = getEndpointJson(ENDPOINT_ACADEMIC_SUBJECT_DESCRIPTOR, config('SILVER_DATA_LOCATION'), school_year)
    student_section_association_content = getEndpointJson(ENDPOINT_STUDENT_SECTION_ASSOCIATIONS, config('SILVER_DATA_LOCATION'), school_year)
    staff_content = getEndpointJson(ENDPOINT_STAFF, config('SILVER_DATA_LOCATION'), school_year)
    staff_section_association_content = getEndpointJson(ENDPOINT_STAFF_SECTION_ASSOCIATIONS, config('SILVER_DATA_LOCATION'), school_year)
    course_content = getEndpointJson(ENDPOINT_COURSE, config('SILVER_DATA_LOCATION'), school_year)
    course_offering_content = getEndpointJson(ENDPOINT_COURSE_OFFERING, config('SILVER_DATA_LOCATION'), school_year)
    sections_content = getEndpointJson(ENDPOINT_SECTIONS, config('SILVER_DATA_LOCATION'), school_year)
    ############################
    # Student Sections Association
    ############################
    student_section_association_normalized = jsonNormalize(
        student_section_association_content,
        recordPath=None,
        meta=[
            'studentReference.studentUniqueId',
            'studentReference.link.href',
            'sectionReference.link.href',
            'sectionReference.schoolId',
            'sectionReference.localCourseCode',
            'sectionReference.schoolYear',
            'sectionReference.sectionIdentifier',
            'sectionReference.sessionName',
            'beginDate',
            'endDate'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    get_reference_from_href(
        student_section_association_normalized,
        'studentReference.link.href',
        'studentReferenceId',
    )
    get_reference_from_href(
        student_section_association_normalized,
        'sectionReference.link.href',
        'sectionReferenceId',
    )
    student_section_association_normalized['StudentSectionStartDateKey'] = to_datetime_key(
        student_section_association_normalized,
        'beginDate'
    )
    student_section_association_normalized['StudentSectionEndDateKey'] = to_datetime_key(
        student_section_association_normalized,
        'endDate'
    )
    student_section_association_normalized = renameColumns(
        student_section_association_normalized,
        {
            'beginDate': 'BeginDate',
            'studentReference.studentUniqueId': 'StudentKey',
            'sectionReference.schoolId': 'SchoolKey',
            'sectionReference.localCourseCode': 'LocalCourseCode',
            'sectionReference.schoolYear': 'SchoolYear',
            'sectionReference.sectionIdentifier': 'SectionIdentifier',
            'sectionReference.sessionName': 'SessionName',
        }
    )
    ############################
    # Sections
    ############################
    sections_normalized = jsonNormalize(
        sections_content,
        recordPath=None,
        meta=[
            'id',
            ['courseOfferingReference', 'link', 'href'],
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    sections_normalized = renameColumns(
        sections_normalized,
        {
            'id': 'sectionReferenceId',
        }
    )
    get_reference_from_href(
        sections_normalized,
        'courseOfferingReference.link.href',
        'courseOfferingReferenceId'
    )
    ############################
    # CourseOffering
    ############################
    course_offering_normalized = jsonNormalize(
        course_offering_content,
        recordPath=None,
        meta=[
            'id',
            ['courseReference', 'link', 'href']
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    get_reference_from_href(
        course_offering_normalized,
        'courseReference.link.href',
        'courseReferenceId'
    )
    course_offering_normalized = renameColumns(
        course_offering_normalized,
        {
            'id': 'courseOfferingReferenceId'
        }
    )
    ############################
    # Course
    ############################
    course_normalized = jsonNormalize(
        course_content,
        recordPath=None,
        meta=[
            'id',
            'academicSubjectDescriptor',
            'courseTitle'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    # Get Descriptor
    get_descriptor_code_value_from_uri(
        course_normalized,
        'academicSubjectDescriptor'
    )
    course_normalized = renameColumns(
        course_normalized,
        {
            'id': 'courseReferenceId',
            'academicSubjectDescriptor': 'academicSubjectDescriptorCodeValue',
            'courseTitle': 'CourseTitle'
        }
    )
    ############################
    # academic_subject_descriptor
    ############################
    academic_subject_descriptor_normalized = jsonNormalize(
        academic_subject_descriptor_content,
        recordPath=None,
        meta=[
            'codeValue',
            'description'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    academic_subject_descriptor_normalized = renameColumns(
        academic_subject_descriptor_normalized,
        {
            'codeValue': 'academicSubjectDescriptorCodeValue',
            'description': 'Subject'
        }
    )
    ############################
    # Staff Sections Association
    ############################
    staff_section_association_normalized = jsonNormalize(
        staff_section_association_content,
        recordPath=None,
        meta=[
            'staffReference.link.href',
            'sectionReference.link.href',
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    get_reference_from_href(
        staff_section_association_normalized,
        'staffReference.link.href',
        'staffReferenceId',
    )
    get_reference_from_href(
        staff_section_association_normalized,
        'sectionReference.link.href',
        'sectionReferenceId',
    )
    ############################
    # Staff
    ############################
    staff_normalized = jsonNormalize(
        staff_content,
        recordPath=None,
        meta=[
            'id',
            'firstName',
            'lastSurname'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    staff_normalized = renameColumns(
        staff_normalized,
        {
            'id': 'staffReferenceId',
        }
    )
    ############################
    # Join Student and Section
    ############################
    if not (staff_section_association_normalized is None or staff_section_association_normalized.empty):
        staff_normalized = pdMerge(
            left=staff_normalized,
            right=staff_section_association_normalized,
            how='inner',
            leftOn=['staffReferenceId'],
            rightOn=['staffReferenceId'],
            suffixLeft=None,
            suffixRight=None
        )
        staff_normalized['TeacherName'] = (
            staff_normalized['firstName']
            + ' ' + staff_normalized['lastSurname']
        )
        staff_normalized = (
            staff_normalized.groupby(['sectionReferenceId'], as_index=False).agg({'TeacherName': ', '.join})
        )
    else:
        staff_normalized['TeacherName'] = ''
        staff_normalized['sectionReferenceId'] = ''
    staff_normalized = subset(
        staff_normalized,
        [
            'TeacherName',
            'sectionReferenceId'
        ]
    )
    ############################
    # Join Student and Section
    ############################
    result_data_frame = pdMerge(
        left=student_section_association_normalized,
        right=sections_normalized,
        how='inner',
        leftOn=['sectionReferenceId'],
        rightOn=['sectionReferenceId'],
        suffixLeft=None,
        suffixRight=None
    )
    if (staff_section_association_normalized is None or staff_section_association_normalized.empty):
        return None
    ############################
    # Join Staff and Section
    ############################
    result_data_frame = pdMerge(
        left=result_data_frame,
        right=staff_normalized,
        how='left',
        leftOn=['sectionReferenceId'],
        rightOn=['sectionReferenceId'],
        suffixLeft=None,
        suffixRight=None
    )
    ############################
    # Join Section and Course Offering
    ############################
    result_data_frame = pdMerge(
        left=result_data_frame,
        right=course_offering_normalized,
        how='inner',
        leftOn=['courseOfferingReferenceId'],
        rightOn=['courseOfferingReferenceId'],
        suffixLeft=None,
        suffixRight=None
    )
    if (result_data_frame is None or result_data_frame.empty):
        return None
    ############################
    # Join Section and Course
    ############################
    result_data_frame = pdMerge(
        left=result_data_frame,
        right=course_normalized,
        how='inner',
        leftOn=['courseReferenceId'],
        rightOn=['courseReferenceId'],
        suffixLeft=None,
        suffixRight=None
    )
    if (result_data_frame is None or result_data_frame.empty):
        return None
    ############################
    # Join Section and academicSubjectDescriptor
    ############################
    result_data_frame = pdMerge(
        left=result_data_frame,
        right=academic_subject_descriptor_normalized,
        how='inner',
        leftOn=['academicSubjectDescriptorCodeValue'],
        rightOn=['academicSubjectDescriptorCodeValue'],
        suffixLeft=None,
        suffixRight=None
    )
    if (result_data_frame is None or result_data_frame.empty):
        return None
    addColumnIfNotExists(result_data_frame, 'TeacherName', '')
    result_data_frame['StudentKey'] = result_data_frame['StudentKey'].astype(str)
    result_data_frame['SchoolKey'] = result_data_frame['SchoolKey'].astype(str)
    result_data_frame['LocalCourseCode'] = result_data_frame['LocalCourseCode'].astype(str)
    result_data_frame['SchoolYear'] = result_data_frame['SchoolYear'].astype(str)
    result_data_frame['SectionIdentifier'] = result_data_frame['SectionIdentifier'].astype(str)
    result_data_frame['SessionName'] = result_data_frame['SessionName'].astype(str)
    result_data_frame['StudentSectionStartDateKey'] = result_data_frame['StudentSectionStartDateKey'].astype(str)
    result_data_frame['StudentSchoolKey'] = (
        result_data_frame['StudentKey']
        + '-' + result_data_frame['SchoolKey']
    )
    result_data_frame['StudentSectionKey'] = (
        result_data_frame['StudentKey']
        + '-' + result_data_frame['SchoolKey']
        + '-' + result_data_frame['LocalCourseCode']
        + '-' + result_data_frame['SchoolYear']
        + '-' + result_data_frame['SectionIdentifier']
        + '-' + result_data_frame['SessionName']
        + '-' + result_data_frame['StudentSectionStartDateKey']
    )
    result_data_frame['StudentSchoolKey'] = (
        result_data_frame['StudentKey']
        + '-' + result_data_frame['SchoolKey']
    )
    result_data_frame['SectionKey'] = (
        result_data_frame['SchoolKey']
        + '-' + result_data_frame['LocalCourseCode']
        + '-' + result_data_frame['SchoolYear']
        + '-' + result_data_frame['SectionIdentifier']
        + '-' + result_data_frame['SessionName']
    )
    # Select columns
    result_data_frame = result_data_frame[
        columns
    ]
    return result_data_frame


def student_section_dim(school_year) -> None:
    return section_dim_dataframe(
        file_name="studentSectionDim.parquet",
        columns=RESULT_COLUMNS,
        school_year=school_year
    )

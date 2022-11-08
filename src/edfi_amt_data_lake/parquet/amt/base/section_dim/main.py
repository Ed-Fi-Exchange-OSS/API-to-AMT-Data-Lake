# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import pandas as pd
from decouple import config

from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    get_reference_from_href,
    jsonNormalize,
    pdMerge,
    renameColumns,
    saveParquetFile,
)

ENDPOINT_ACADEMIC_SUBJECT_DESCRIPTOR = 'academicSubjectDescriptors'
ENDPOINT_EDUCATIONAL_ENVIRONMENT_DESCRIPTOR = 'educationalEnvironmentDescriptors'
ENDPOINT_TERM_DESCRIPTOR = 'termDescriptors'
ENDPOINT_COURSES = 'courses'
ENDPOINT_COURSE_OFFERINGS = 'courseOfferings'
ENDPOINT_SCHOOLS = 'schools'
ENDPOINT_SECTIONS = 'sections'
ENDPOINT_SESSIONS = 'sessions'


def section_dim_dataframe(school_year) -> pd.DataFrame:
    academic_subject_descriptor_content = getEndpointJson(ENDPOINT_ACADEMIC_SUBJECT_DESCRIPTOR, config('SILVER_DATA_LOCATION'), school_year)
    term_descriptor_content = getEndpointJson(ENDPOINT_TERM_DESCRIPTOR, config('SILVER_DATA_LOCATION'), school_year)
    educational_environment_descriptor_content = getEndpointJson(ENDPOINT_EDUCATIONAL_ENVIRONMENT_DESCRIPTOR, config('SILVER_DATA_LOCATION'), school_year)
    courses_content = getEndpointJson(ENDPOINT_COURSES, config('SILVER_DATA_LOCATION'), school_year)
    courses_offerings_content = getEndpointJson(ENDPOINT_COURSE_OFFERINGS, config('SILVER_DATA_LOCATION'), school_year)
    schools_content = getEndpointJson(ENDPOINT_SCHOOLS, config('SILVER_DATA_LOCATION'), school_year)
    sections_content = getEndpointJson(ENDPOINT_SECTIONS, config('SILVER_DATA_LOCATION'), school_year)
    sessions_content = getEndpointJson(ENDPOINT_SESSIONS, config('SILVER_DATA_LOCATION'), school_year)

    academic_subject_descriptor_normalized = jsonNormalize(
        academic_subject_descriptor_content,
        recordPath=None,
        meta=[
            'id',
            'academicSubjectDescriptorId',
            'codeValue',
            'description',
            'namespace'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    academic_subject_descriptor_normalized['namespaceWithCodeValue'] = (
        academic_subject_descriptor_normalized['namespace'] + '#' + academic_subject_descriptor_normalized['codeValue']
    )

    term_descriptor_normalized = jsonNormalize(
        term_descriptor_content,
        recordPath=None,
        meta=[
            'id',
            'termDescriptorId',
            'codeValue',
            'description',
            'namespace'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    term_descriptor_normalized['namespaceWithCodeValue'] = (
        term_descriptor_normalized['namespace'] + '#' + term_descriptor_normalized['codeValue']
    )

    educational_environment_descriptor_normalized = jsonNormalize(
        educational_environment_descriptor_content,
        recordPath=None,
        meta=[
            'id',
            'educationalEnvironmentDescriptorId',
            'codeValue',
            'description',
            'namespace'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    educational_environment_descriptor_normalized['namespaceWithCodeValue'] = (
        educational_environment_descriptor_normalized['namespace'] + '#' + educational_environment_descriptor_normalized['codeValue']
    )

    sections_normalized = jsonNormalize(
        sections_content,
        recordPath=None,
        meta=[
            'id',
            ['courseOfferingReference', 'schoolId'],
            ['courseOfferingReference', 'localCourseCode'],
            ['courseOfferingReference', 'schoolYear'],
            ['courseOfferingReference', 'sessionName'],
            ['courseOfferingReference', 'link', 'href'],
            'sectionIdentifier',
            'educationalEnvironmentDescriptor',
            'sectionName'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    get_reference_from_href(
        sections_normalized,
        'courseOfferingReference.link.href',
        'courseOfferingReferenceId'
    )

    sections_class_periods_normalized = jsonNormalize(
        sections_content,
        recordPath=['classPeriods'],
        meta=[
            'id'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    courses_offerings_normalized = jsonNormalize(
        courses_offerings_content,
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
        courses_offerings_normalized,
        'courseReference.link.href',
        'courseReferenceId'
    )

    get_reference_from_href(
        courses_offerings_normalized,
        'sessionReference.link.href',
        'sessionReferenceId'
    )

    get_reference_from_href(
        courses_offerings_normalized,
        'schoolReference.link.href',
        'schoolReferenceId'
    )

    courses_normalized = jsonNormalize(
        courses_content,
        recordPath=None,
        meta=[
            'id',
            ['educationOrganizationReference', 'educationOrganizationId'],
            'courseCode',
            'academicSubjectDescriptor'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    sessions_normalized = jsonNormalize(
        sessions_content,
        recordPath=None,
        meta=[
            'id',
            ['termDescriptor']
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    schools_normalized = jsonNormalize(
        schools_content,
        recordPath=None,
        meta=[
            'id',
            ['localEducationAgencyReference', 'localEducationAgencyId'],
            'schoolId'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    result_data_frame = pdMerge(
        left=sections_normalized,
        right=courses_offerings_normalized,
        how='inner',
        leftOn=['courseOfferingReferenceId'],
        rightOn=['id'],
        suffixLeft=None,
        suffixRight='_courseOfferings'
    )

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=courses_normalized,
        how='inner',
        leftOn=['courseReferenceId'],
        rightOn=['id'],
        suffixLeft=None,
        suffixRight='_courses'
    )

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=sections_class_periods_normalized,
        how='left',
        leftOn=['id'],
        rightOn=['id'],
        suffixLeft=None,
        suffixRight='_classPeriods'
    )

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=sessions_normalized,
        how='left',
        leftOn=['sessionReferenceId'],
        rightOn=['id'],
        suffixLeft=None,
        suffixRight='_sessions'
    )

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=schools_normalized,
        how='left',
        leftOn=['courseOfferingReference.schoolId'],
        rightOn=['schoolId'],
        suffixLeft=None,
        suffixRight='_schools'
    )

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=academic_subject_descriptor_normalized,
        how='inner',
        leftOn=['academicSubjectDescriptor'],
        rightOn=['namespaceWithCodeValue'],
        suffixLeft=None,
        suffixRight='_academic_subj_desc'
    )

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=term_descriptor_normalized,
        how='inner',
        leftOn=['termDescriptor'],
        rightOn=['namespaceWithCodeValue'],
        suffixLeft=None,
        suffixRight='_term_desc'
    )

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=educational_environment_descriptor_normalized,
        how='inner',
        leftOn=['educationalEnvironmentDescriptor'],
        rightOn=['namespaceWithCodeValue'],
        suffixLeft=None,
        suffixRight='_educational_environment_desc'
    )

    result_data_frame = result_data_frame[[
        'sectionIdentifier',
        'courseOfferingReference.localCourseCode',
        'courseOfferingReference.schoolId',
        'courseOfferingReference.schoolYear',
        'courseOfferingReference.sessionName',
        'academicSubjectDescriptor',
        'courseTitle',
        'classPeriodReference.classPeriodName',
        'termDescriptor',
        'educationalEnvironmentDescriptor',
        'localEducationAgencyReference.localEducationAgencyId',
        'description',
        'description_term_desc',
        'description_educational_environment_desc'
    ]]

    result_data_frame['courseOfferingReference.schoolId'] = result_data_frame['courseOfferingReference.schoolId'].astype(str)
    result_data_frame['courseOfferingReference.schoolYear'] = result_data_frame['courseOfferingReference.schoolYear'].astype(str)

    result_data_frame['SectionKey'] = (
        result_data_frame['courseOfferingReference.schoolId']
        + '-'
        + result_data_frame['courseOfferingReference.localCourseCode']
        + '-'
        + result_data_frame['courseOfferingReference.schoolYear']
        + '-'
        + result_data_frame['sectionIdentifier']
        + '-'
        + result_data_frame['courseOfferingReference.sessionName']
    )

    result_data_frame['Description'] = (
        result_data_frame['description']
        + '-('
        + result_data_frame['courseOfferingReference.localCourseCode']
        + ')-'
        + result_data_frame['courseTitle']
        + '-('
        + result_data_frame['classPeriodReference.classPeriodName']
        + ')-'
        + result_data_frame['description_term_desc']
    )

    result_data_frame['SectionName'] = (
        result_data_frame['courseOfferingReference.localCourseCode']
        + '-'
        + result_data_frame['courseOfferingReference.sessionName']
    )

    result_data_frame['SessionKey'] = (
        result_data_frame['courseOfferingReference.schoolId']
        + '-'
        + result_data_frame['courseOfferingReference.schoolYear']
        + '-'
        + result_data_frame['courseOfferingReference.sessionName']
    )

    result_data_frame = renameColumns(result_data_frame, {
        'courseOfferingReference.schoolId': 'SchoolKey',
        'courseOfferingReference.sessionName': 'SessionName',
        'courseOfferingReference.localCourseCode': 'LocalCourseCode',
        'courseOfferingReference.schoolYear': 'SchoolYear',
        'description_educational_environment_desc': 'EducationalEnvironmentDescriptor',
        'localEducationAgencyReference.localEducationAgencyId': 'LocalEducationAgencyKey',
        'courseTitle': 'CourseTitle'
    })

    result_data_frame = result_data_frame[[
        'SchoolKey',
        'SectionKey',
        'Description',
        'SectionName',
        'SessionName',
        'LocalCourseCode',
        'SchoolYear',
        'EducationalEnvironmentDescriptor',
        'LocalEducationAgencyKey',
        'CourseTitle',
        'SessionKey'
    ]]

    return result_data_frame


def section_dim(school_year) -> None:
    result_data_frame = section_dim_dataframe(school_year)
    saveParquetFile(result_data_frame, f"{config('PARQUET_FILES_LOCATION')}", "sectionDim.parquet", school_year)

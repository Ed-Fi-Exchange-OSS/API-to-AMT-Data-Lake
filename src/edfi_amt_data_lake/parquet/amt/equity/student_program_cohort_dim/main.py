# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from datetime import date

from decouple import config

from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    get_descriptor_code_value_from_uri,
    get_reference_from_href,
    jsonNormalize,
    pdMerge,
    renameColumns,
    saveParquetFile,
    subset,
    to_datetime_key,
)

ENDPOINT_COHORT = 'cohorts'
ENDPOINT_STUDENT_COHORT_ASSOCIATION = 'studentCohortAssociations'
ENDPOINT_STUDENT_SCHOOL_ASSOCIATION = 'studentSchoolAssociations'
ENDPOINT_PROGRAM_TYPE_DESCRIPTOR = 'programTypeDescriptors'
ENDPOINT_COHORT_TYPE_DESCRIPTOR = 'cohortTypeDescriptors'
ENDPOINT_GRADE_LEVEL_DESCRIPTOR = 'gradeLevelDescriptors'


def student_program_cohort_dim(school_year) -> None:
    student_cohort_association_content = getEndpointJson(ENDPOINT_STUDENT_COHORT_ASSOCIATION, config('SILVER_DATA_LOCATION'), school_year)
    cohort_content = getEndpointJson(ENDPOINT_COHORT, config('SILVER_DATA_LOCATION'), school_year)
    student_school_association_content = getEndpointJson(ENDPOINT_STUDENT_SCHOOL_ASSOCIATION, config('SILVER_DATA_LOCATION'), school_year)
    cohort_type_descriptor_content = getEndpointJson(ENDPOINT_COHORT_TYPE_DESCRIPTOR, config('SILVER_DATA_LOCATION'), school_year)
    grade_level_descriptor_content = getEndpointJson(ENDPOINT_GRADE_LEVEL_DESCRIPTOR, config('SILVER_DATA_LOCATION'), school_year)
    program_type_descriptor_content = getEndpointJson(ENDPOINT_PROGRAM_TYPE_DESCRIPTOR, config('SILVER_DATA_LOCATION'), school_year)

    ############################
    # studentCohortAssociations
    ############################
    student_cohort_association_normalized = jsonNormalize(
        student_cohort_association_content,
        recordPath=None,
        meta=[
            'cohortReference.link.href',
            'studentReference.link.href',
            'studentReference.studentUniqueId',
            'beginDate'
        ],
        metaPrefix=None,
        recordPrefix='student_program_',
        errors='ignore'
    )

    get_reference_from_href(student_cohort_association_normalized, 'cohortReference.link.href', 'cohort_id')
    get_reference_from_href(student_cohort_association_normalized, 'studentReference.link.href', 'student_id')

    student_cohort_association_normalized = renameColumns(student_cohort_association_normalized, {
        'studentReference.studentUniqueId' : 'studentUniqueId'
    })
    # Select needed columns.
    student_cohort_association_normalized = subset(student_cohort_association_normalized, [
        'cohort_id',
        'student_id',
        'beginDate',
        'studentUniqueId'
    ])
    ############################
    # cohort_content
    ############################
    cohort_normalized = jsonNormalize(
        cohort_content,
        recordPath='programs',
        meta=[
            'id',
            'cohortIdentifier',
            'cohortDescription',
            'cohortTypeDescriptor',
            ['educationOrganizationReference', 'educationOrganizationId'],
        ],
        metaPrefix=None,
        recordPrefix='cohort_',
        errors='ignore'
    )
    cohort_normalized = renameColumns(cohort_normalized, {
        'id' : 'cohort_id',
        'cohort_programReference.educationOrganizationId' : 'programEducationOrganizationId',
        'cohort_programReference.programName' : 'programName',
        'cohort_programReference.programTypeDescriptor' : 'programTypeDescriptorCodeValue',
        'educationOrganizationReference.educationOrganizationId' : 'educationOrganizationId',
        'programReference.educationOrganizationId': 'programEducationOrganizationId',
        'cohort_programReference.link.href' : 'programHref',
        'cohortTypeDescriptor': 'cohortTypeDescriptorCodeValue'
    })

    get_descriptor_code_value_from_uri(cohort_normalized, 'programTypeDescriptorCodeValue')
    get_descriptor_code_value_from_uri(cohort_normalized, 'cohortTypeDescriptorCodeValue')

    student_cohort_association_normalized = pdMerge(
        left=student_cohort_association_normalized,
        right=cohort_normalized,
        how='inner',
        leftOn=['cohort_id'],
        rightOn=['cohort_id'],
        suffixLeft='_student_cohort_association',
        suffixRight='_cohort'
    )

    # Select needed columns.
    student_cohort_association_normalized = subset(student_cohort_association_normalized, [
        'programEducationOrganizationId',
        'programName',
        'programTypeDescriptorCodeValue',
        'cohortIdentifier',
        'cohortDescription',
        'educationOrganizationId',
        'studentUniqueId',
        'beginDate',
        'cohortTypeDescriptorCodeValue'
    ])

    ############################
    # cohortTypeDescriptor
    ############################
    cohort_type_descriptor_association_normalized = jsonNormalize(
        cohort_type_descriptor_content,
        recordPath=None,
        meta=[
            'cohortTypeDescriptorId',
            'codeValue',
            'description'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    cohort_type_descriptor_association_normalized = subset(cohort_type_descriptor_association_normalized, [
        'cohortTypeDescriptorId',
        'codeValue',
        'description'
    ])

    cohort_type_descriptor_association_normalized = renameColumns(cohort_type_descriptor_association_normalized, {
        'codeValue': 'cohortTypeDescriptorCodeValue',
        'description': 'cohortTypeDescriptorDescription'
    })

    student_cohort_association_normalized = pdMerge(
        left=student_cohort_association_normalized,
        right=cohort_type_descriptor_association_normalized,
        how='inner',
        leftOn=['cohortTypeDescriptorCodeValue'],
        rightOn=['cohortTypeDescriptorCodeValue'],
        suffixLeft='_student_cohort_association',
        suffixRight='_cohort'
    )
    ############################
    # programTypeDescriptor
    ############################
    program_type_descriptor_association_normalized = jsonNormalize(
        program_type_descriptor_content,
        recordPath=None,
        meta=[
            'programTypeDescriptorId',
            'codeValue',
            'description'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    program_type_descriptor_association_normalized = subset(program_type_descriptor_association_normalized, [
        'programTypeDescriptorId',
        'codeValue',
        'description'
    ])

    program_type_descriptor_association_normalized = renameColumns(program_type_descriptor_association_normalized, {
        'codeValue': 'programTypeDescriptorCodeValue',
        'description': 'programTypeDescriptorDescription'
    })

    student_cohort_association_normalized = pdMerge(
        left=student_cohort_association_normalized,
        right=program_type_descriptor_association_normalized,
        how='inner',
        leftOn=['programTypeDescriptorCodeValue'],
        rightOn=['programTypeDescriptorCodeValue'],
        suffixLeft='_student_cohort_association',
        suffixRight='_program_type'
    )
    ############################
    # studentSchoolAssociation
    ############################
    student_school_association_normalized = jsonNormalize(
        student_school_association_content,
        recordPath=None,
        meta=[
            'schoolReference.schoolId',
            'studentReference.studentUniqueId',
            'entryDate',
            'exitWithdrawDate',
            'entryGradeLevelDescriptor'
        ],
        metaPrefix=None,
        recordPrefix='studentSchoolAssociation_',
        errors='ignore'
    )
    student_school_association_normalized['exitWithdrawDate'] = to_datetime_key(student_school_association_normalized, 'exitWithdrawDate')
    student_school_association_normalized['date_now'] = date.today()
    student_school_association_normalized['date_now'] = to_datetime_key(student_school_association_normalized, 'date_now')

    # Select needed columns.
    student_school_association_normalized = subset(student_school_association_normalized, [
        'schoolReference.schoolId',
        'studentReference.studentUniqueId',
        'exitWithdrawDate',
        'date_now',
        'entryGradeLevelDescriptor'
    ])

    student_school_association_normalized = renameColumns(student_school_association_normalized, {
        'schoolReference.schoolId': 'schoolId',
        'studentReference.studentUniqueId': 'studentUniqueId',
        'entryGradeLevelDescriptor' : 'gradeLevelDescriptorCodeValue'
    })

    get_descriptor_code_value_from_uri(student_school_association_normalized, 'gradeLevelDescriptorCodeValue')

    ############################
    # gradeLevelDescriptor
    ############################
    grade_level_descriptor_normalized = jsonNormalize(
        grade_level_descriptor_content,
        recordPath=None,
        meta=[
            'gradeLevelDescriptorId',
            'codeValue',
            'description'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    grade_level_descriptor_normalized = subset(grade_level_descriptor_normalized, [
        'gradeLevelDescriptorId',
        'codeValue',
        'description'
    ])

    grade_level_descriptor_normalized = renameColumns(grade_level_descriptor_normalized, {
        'codeValue': 'gradeLevelDescriptorCodeValue',
        'description': 'gradeLevelDescriptorDescription'
    })

    student_school_association_normalized = pdMerge(
        left=student_school_association_normalized,
        right=grade_level_descriptor_normalized,
        how='inner',
        leftOn=['gradeLevelDescriptorCodeValue'],
        rightOn=['gradeLevelDescriptorCodeValue'],
        suffixLeft='_student_cohort_association',
        suffixRight='_grade_level'
    )
    ############################
    # student_school_association - student_school_association
    ############################
    result_data_frame = pdMerge(
        left=student_cohort_association_normalized,
        right=student_school_association_normalized,
        how='inner',
        leftOn=['studentUniqueId'],
        rightOn=['studentUniqueId'],
        suffixLeft='_discipline_action',
        suffixRight='_student_school_association'
    )
    # Filter by exitWithdrawDate
    result_data_frame = result_data_frame[result_data_frame['exitWithdrawDate'] >= result_data_frame['date_now']]
    result_data_frame['beginDateKey'] = to_datetime_key(result_data_frame, 'beginDate')
    result_data_frame['studentKey'] = result_data_frame['studentUniqueId'].astype(str)
    result_data_frame['schoolKey'] = result_data_frame['schoolId'].astype(str)
    result_data_frame['educationOrganizationId'] = result_data_frame['educationOrganizationId'].astype(str)
    result_data_frame['programEducationOrganizationId'] = result_data_frame['programEducationOrganizationId'].astype(str)
    result_data_frame['educationOrganizationId'] = result_data_frame['educationOrganizationId'].astype(str)
    result_data_frame['beginDateKey'] = result_data_frame['beginDateKey'].astype(str)
    result_data_frame['cohortIdentifier'] = result_data_frame['cohortIdentifier'].astype(str)
    result_data_frame['programTypeDescriptorId'] = result_data_frame['programTypeDescriptorId'].astype(str)

    result_data_frame['studentProgramCohortKey'] = (
        result_data_frame['studentKey']
        + '-' + result_data_frame['schoolKey']
        + '-' + result_data_frame['programName']
        + '-' + result_data_frame['programTypeDescriptorId']
        + '-' + result_data_frame['educationOrganizationId']
        + '-' + result_data_frame['programEducationOrganizationId']
        + '-' + result_data_frame['beginDateKey']
        + '-' + result_data_frame['cohortIdentifier']
    )

    result_data_frame['studentSchoolProgramKey'] = (
        result_data_frame['studentKey']
        + '-' + result_data_frame['schoolKey']
        + '-' + result_data_frame['programName']
        + '-' + result_data_frame['programTypeDescriptorId']
        + '-' + result_data_frame['educationOrganizationId']
        + '-' + result_data_frame['programEducationOrganizationId']
        + '-' + result_data_frame['beginDateKey']
    )

    result_data_frame['studentSchoolKey'] = (
        result_data_frame['studentKey']
        + '-' + result_data_frame['schoolKey']
    )
    result_data_frame['cohortTypeDescriptor'] = result_data_frame['cohortTypeDescriptorDescription']
    result_data_frame['entryGradeLevelDescriptor'] = result_data_frame['gradeLevelDescriptorDescription']

    result_data_frame = subset(result_data_frame, [
        'studentProgramCohortKey',
        'studentSchoolProgramKey',
        'studentSchoolKey',
        'entryGradeLevelDescriptor',
        'cohortTypeDescriptor',
        'cohortDescription',
        'programName'
    ])

    saveParquetFile(result_data_frame, f"{config('PARQUET_FILES_LOCATION')}", "equity_StudentProgramCohortDim.parquet", school_year)

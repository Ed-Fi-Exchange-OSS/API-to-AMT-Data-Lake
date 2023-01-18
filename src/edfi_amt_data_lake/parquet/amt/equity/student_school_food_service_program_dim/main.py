# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from datetime import date

from decouple import config

from edfi_amt_data_lake.helper.data_frame_generation_result import (
    data_frame_generation_result,
)
from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    create_parquet_file,
    get_descriptor_code_value_from_uri,
    is_data_frame_empty,
    jsonNormalize,
    pdMerge,
    renameColumns,
    subset,
    to_datetime_key,
)

ENDPOINT_STUDENT_FOOD_SERVICE_PROGRAM_ASSOCIATION = 'studentSchoolFoodServiceProgramAssociations'
ENDPOINT_STUDENT_SCHOOL_ASSOCIATION = 'studentSchoolAssociations'
ENDPOINT_PROGRAM_TYPE_DESCRIPTOR = 'programTypeDescriptors'
ENDPOINT_SCHOOL_FOOD_SERVICE_PROGRAM_SERVICE_DESCRIPTOR = 'schoolFoodServiceProgramServiceDescriptors'
RESULT_COLUMNS = [
    'StudentSchoolFoodServiceProgramKey',
    'StudentSchoolProgramKey',
    'StudentSchoolKey',
    'ProgramName',
    'SchoolFoodServiceProgramServiceDescriptor'
]


@create_parquet_file
def student_school_food_service_program_dim_frame(
    file_name: str,
    columns: list[str],
    school_year: int
):
    file_name = file_name
    studentFoodServiceProgramAssociationContent = getEndpointJson(ENDPOINT_STUDENT_FOOD_SERVICE_PROGRAM_ASSOCIATION, config('SILVER_DATA_LOCATION'), school_year)
    studentSchoolAssociationsContent = getEndpointJson(ENDPOINT_STUDENT_SCHOOL_ASSOCIATION, config('SILVER_DATA_LOCATION'), school_year)
    programTypeDescriptorContent = getEndpointJson(ENDPOINT_PROGRAM_TYPE_DESCRIPTOR, config('SILVER_DATA_LOCATION'), school_year)
    schoolFoodServiceProgramServiceDescriptorContent = getEndpointJson(ENDPOINT_SCHOOL_FOOD_SERVICE_PROGRAM_SERVICE_DESCRIPTOR, config('SILVER_DATA_LOCATION'), school_year)

    # studentFoodServiceProgramAssociationContent
    studentFoodServiceProgramAssociationNormalized = jsonNormalize(
        studentFoodServiceProgramAssociationContent,
        recordPath=['schoolFoodServiceProgramServices'],
        meta=[
            ['studentReference', 'studentUniqueId'],
            ['programReference', 'programName'],
            ['programReference', 'programTypeDescriptor'],
            ['programReference', 'educationOrganizationId'],
            'beginDate',
            'schoolFoodServiceProgramServiceDescriptor',
            ['educationOrganizationReference', 'educationOrganizationId']
        ],
        recordMeta=['schoolFoodServiceProgramServiceDescriptor'],
        metaPrefix=None,
        recordPrefix='schoolFoodServiceProgramServices_',
        errors='ignore'
    )

    # Select needed columns.
    studentFoodServiceProgramAssociationNormalized = subset(studentFoodServiceProgramAssociationNormalized, [
        'studentReference.studentUniqueId',
        'programReference.programName',
        'programReference.programTypeDescriptor',
        'programReference.educationOrganizationId',
        'beginDate',
        'schoolFoodServiceProgramServices_schoolFoodServiceProgramServiceDescriptor',
        'educationOrganizationReference.educationOrganizationId'
    ])

    studentFoodServiceProgramAssociationNormalized = renameColumns(studentFoodServiceProgramAssociationNormalized, {
        'studentReference.studentUniqueId': 'studentUniqueId',
        'programReference.programName': 'programName',
        'programReference.programTypeDescriptor': 'programTypeDescriptor',
        'programReference.educationOrganizationId': 'programEducationOrganizationId',
        'schoolFoodServiceProgramServices_schoolFoodServiceProgramServiceDescriptor': 'schoolFoodServiceProgramServiceDescriptor',
        'educationOrganizationReference.educationOrganizationId': 'educationOrganizationId'
    })

    # Remove namespace
    get_descriptor_code_value_from_uri(studentFoodServiceProgramAssociationNormalized, 'programTypeDescriptor')

    # Remove namespace
    get_descriptor_code_value_from_uri(studentFoodServiceProgramAssociationNormalized, 'schoolFoodServiceProgramServiceDescriptor')

    ############################
    # programTypeDescriptor
    ############################
    programTypeDescriptorNormalized = jsonNormalize(
        programTypeDescriptorContent,
        recordPath=None,
        meta=[
            'programTypeDescriptorId',
            'codeValue'
        ],
        metaPrefix=None,
        recordPrefix='programTypeDescriptor_',
        errors='ignore'
    )

    # Select needed columns.
    programTypeDescriptorNormalized = subset(programTypeDescriptorNormalized, [
        'programTypeDescriptorId',
        'codeValue'
    ])

    programTypeDescriptorNormalized = renameColumns(programTypeDescriptorNormalized, {
        'codeValue': 'programTypeDescriptor'
    })

    studentFoodServiceProgramAssociationNormalized = pdMerge(
        left=studentFoodServiceProgramAssociationNormalized,
        right=programTypeDescriptorNormalized,
        how='left',
        leftOn=['programTypeDescriptor'],
        rightOn=['programTypeDescriptor'],
        suffixLeft='_studentSchoolAssociation',
        suffixRight='_studentFoodServiceProgramAssociation'
    )

    # schoolFoodServiceProgramServiceDescriptor
    schoolFoodServiceProgramServiceDescriptorNormalized = jsonNormalize(
        schoolFoodServiceProgramServiceDescriptorContent,
        recordPath=None,
        meta=[
            'schoolFoodServiceProgramServiceDescriptorId',
            'codeValue'
        ],
        metaPrefix=None,
        recordPrefix='schoolFoodServiceProgramServiceDescriptor_',
        errors='ignore'
    )

    # Select needed columns.
    schoolFoodServiceProgramServiceDescriptorNormalized = subset(schoolFoodServiceProgramServiceDescriptorNormalized, [
        'schoolFoodServiceProgramServiceDescriptorId',
        'codeValue'
    ])

    schoolFoodServiceProgramServiceDescriptorNormalized = renameColumns(schoolFoodServiceProgramServiceDescriptorNormalized, {
        'codeValue': 'schoolFoodServiceProgramServiceDescriptor'
    })

    studentFoodServiceProgramAssociationNormalized = pdMerge(
        left=studentFoodServiceProgramAssociationNormalized,
        right=schoolFoodServiceProgramServiceDescriptorNormalized,
        how='left',
        leftOn=['schoolFoodServiceProgramServiceDescriptor'],
        rightOn=['schoolFoodServiceProgramServiceDescriptor'],
        suffixLeft='_studentSchoolAssociation',
        suffixRight='_studentFoodServiceProgramAssociation'
    )

    # studentSchoolAssociation
    studentSchoolAssociationNormalized = jsonNormalize(
        studentSchoolAssociationsContent,
        recordPath=None,
        meta=[
            'schoolReference.schoolId',
            'studentReference.studentUniqueId',
            'entryDate',
            'exitWithdrawDate'
        ],
        metaPrefix=None,
        recordPrefix='studentSchoolAssociation_',
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

    # studentFoodServiceProgramAssociationContent - studentSchoolAssociations
    result_data_frame = pdMerge(
        left=studentSchoolAssociationNormalized,
        right=studentFoodServiceProgramAssociationNormalized,
        how='inner',
        leftOn=['studentUniqueId'],
        rightOn=['studentUniqueId'],
        suffixLeft='_studentSchoolAssociation',
        suffixRight='_studentFoodServiceProgramAssociation'
    )
    if is_data_frame_empty(result_data_frame):
        return None
    result_data_frame['exitWithdrawDate'] = to_datetime_key(result_data_frame, 'exitWithdrawDate')
    result_data_frame['date_now'] = date.today()
    result_data_frame['date_now'] = to_datetime_key(result_data_frame, 'date_now')
    result_data_frame = result_data_frame[result_data_frame['exitWithdrawDate'] >= result_data_frame['date_now']]

    result_data_frame['studentUniqueId'] = result_data_frame['studentUniqueId'].astype(str)
    result_data_frame['schoolId'] = result_data_frame['schoolId'].astype(str)
    result_data_frame['educationOrganizationId'] = result_data_frame['educationOrganizationId'].astype(str)
    result_data_frame['programEducationOrganizationId'] = result_data_frame['programEducationOrganizationId'].astype(str)
    result_data_frame['schoolFoodServiceProgramServiceDescriptorId'] = result_data_frame['schoolFoodServiceProgramServiceDescriptorId'].astype(str)
    result_data_frame['programTypeDescriptorId'] = result_data_frame['programTypeDescriptorId'].astype(str)
    result_data_frame['beginDate'] = to_datetime_key(result_data_frame, 'beginDate').astype(str)

    result_data_frame['StudentSchoolFoodServiceProgramKey'] = (
        result_data_frame['studentUniqueId']
        + '-' + result_data_frame['schoolId']
        + '-' + result_data_frame['programName']
        + '-' + result_data_frame['programTypeDescriptorId']
        + '-' + result_data_frame['educationOrganizationId']
        + '-' + result_data_frame['programEducationOrganizationId']
        + '-' + result_data_frame['beginDate']
        + '-' + result_data_frame['schoolFoodServiceProgramServiceDescriptorId']
    )

    result_data_frame['StudentSchoolProgramKey'] = (
        result_data_frame['studentUniqueId']
        + '-' + result_data_frame['schoolId']
        + '-' + result_data_frame['programName']
        + '-' + result_data_frame['programTypeDescriptorId']
        + '-' + result_data_frame['educationOrganizationId']
        + '-' + result_data_frame['programEducationOrganizationId']
        + '-' + result_data_frame['beginDate']
    )
    result_data_frame['StudentSchoolKey'] = (
        result_data_frame['studentUniqueId']
        + '-' + result_data_frame['schoolId']
    )
    result_data_frame = renameColumns(
        result_data_frame,
        {
            'programName': 'ProgramName',
            'schoolFoodServiceProgramServiceDescriptor': 'SchoolFoodServiceProgramServiceDescriptor'
        }
    )
    # Select needed columns.
    result_data_frame = subset(result_data_frame, columns)
    return result_data_frame


def student_school_food_service_program_dim(school_year) -> data_frame_generation_result:
    return student_school_food_service_program_dim_frame(
        file_name="equity_StudentSchoolFoodServiceProgramDim.parquet",
        columns=RESULT_COLUMNS,
        school_year=school_year
    )

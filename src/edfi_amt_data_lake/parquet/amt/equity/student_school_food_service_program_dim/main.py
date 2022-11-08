# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from datetime import date

from decouple import config

from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    get_descriptor_code_value_from_uri,
    jsonNormalize,
    pdMerge,
    renameColumns,
    saveParquetFile,
    subset,
    to_datetime_key,
)

ENDPOINT_STUDENT_FOOD_SERVICE_PROGRAM_ASSOCIATION = 'studentSchoolFoodServiceProgramAssociations'
ENDPOINT_STUDENT_SCHOOL_ASSOCIATION = 'studentSchoolAssociations'
ENDPOINT_PROGRAM_TYPE_DESCRIPTOR = 'programTypeDescriptors'
ENDPOINT_SCHOOL_FOOD_SERVICE_PROGRAM_SERVICE_DESCRIPTOR = 'schoolFoodServiceProgramServiceDescriptors'


def student_school_food_service_program_dim(school_year) -> None:
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
    resultDataFrame = pdMerge(
        left=studentSchoolAssociationNormalized,
        right=studentFoodServiceProgramAssociationNormalized,
        how='inner',
        leftOn=['studentUniqueId'],
        rightOn=['studentUniqueId'],
        suffixLeft='_studentSchoolAssociation',
        suffixRight='_studentFoodServiceProgramAssociation'
    )
    resultDataFrame['exitWithdrawDate'] = to_datetime_key(resultDataFrame, 'exitWithdrawDate')
    resultDataFrame['date_now'] = date.today()
    resultDataFrame['date_now'] = to_datetime_key(resultDataFrame, 'date_now')
    resultDataFrame = resultDataFrame[resultDataFrame['exitWithdrawDate'] >= resultDataFrame['date_now']]

    resultDataFrame['studentUniqueId'] = resultDataFrame['studentUniqueId'].astype(str)
    resultDataFrame['schoolId'] = resultDataFrame['schoolId'].astype(str)
    resultDataFrame['educationOrganizationId'] = resultDataFrame['educationOrganizationId'].astype(str)
    resultDataFrame['programEducationOrganizationId'] = resultDataFrame['programEducationOrganizationId'].astype(str)
    resultDataFrame['schoolFoodServiceProgramServiceDescriptorId'] = resultDataFrame['schoolFoodServiceProgramServiceDescriptorId'].astype(str)
    resultDataFrame['programTypeDescriptorId'] = resultDataFrame['programTypeDescriptorId'].astype(str)
    resultDataFrame['beginDate'] = to_datetime_key(resultDataFrame, 'beginDate').astype(str)

    resultDataFrame['StudentSchoolFoodServiceProgramKey'] = (
        resultDataFrame['studentUniqueId']
        + '-' + resultDataFrame['schoolId']
        + '-' + resultDataFrame['programName']
        + '-' + resultDataFrame['programTypeDescriptorId']
        + '-' + resultDataFrame['educationOrganizationId']
        + '-' + resultDataFrame['programEducationOrganizationId']
        + '-' + resultDataFrame['beginDate']
        + '-' + resultDataFrame['schoolFoodServiceProgramServiceDescriptorId']
    )

    resultDataFrame['StudentSchoolProgramKey'] = (
        resultDataFrame['studentUniqueId']
        + '-' + resultDataFrame['schoolId']
        + '-' + resultDataFrame['programName']
        + '-' + resultDataFrame['programTypeDescriptorId']
        + '-' + resultDataFrame['educationOrganizationId']
        + '-' + resultDataFrame['programEducationOrganizationId']
        + '-' + resultDataFrame['beginDate']
    )
    resultDataFrame['StudentSchoolKey'] = (
        resultDataFrame['studentUniqueId']
        + '-' + resultDataFrame['schoolId']
    )

    # Select needed columns.
    resultDataFrame = subset(resultDataFrame, [
        'StudentSchoolFoodServiceProgramKey',
        'StudentSchoolProgramKey',
        'StudentSchoolKey',
        'programName',
        'schoolFoodServiceProgramServiceDescriptor'
    ])

    saveParquetFile(resultDataFrame, f"{config('PARQUET_FILES_LOCATION')}", "equity_StudentSchoolFoodServiceProgramDim.parquet", school_year)

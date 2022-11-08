# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from datetime import date

from decouple import config

from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    addColumnIfNotExists,
    get_descriptor_code_value_from_uri,
    jsonNormalize,
    pdMerge,
    renameColumns,
    saveParquetFile,
    subset,
    to_datetime_key,
)

ENDPOINT_DISCIPLINE_ACTIONS = 'disciplineActions'
ENDPOINT_DISCIPLINE_DESCRIPTOR = 'disciplineDescriptors'
ENDPOINT_STUDENT_SCHOOL_ASSOCIATION = 'studentschoolAssociations'


def student_discipline_action_dim(school_year) -> None:
    discipline_action_content = getEndpointJson(ENDPOINT_DISCIPLINE_ACTIONS, config('SILVER_DATA_LOCATION'), school_year)
    discipline_descriptor_content = getEndpointJson(ENDPOINT_DISCIPLINE_DESCRIPTOR, config('SILVER_DATA_LOCATION'), school_year)
    student_school_association_content = getEndpointJson(ENDPOINT_STUDENT_SCHOOL_ASSOCIATION, config('SILVER_DATA_LOCATION'), school_year)
    ############################
    # disciplineActions
    ############################
    discipline_action_normalized = jsonNormalize(
        discipline_action_content,
        recordPath='disciplines',
        meta=[
            'disciplineActionIdentifier',
            'disciplineDate',
            ['studentReference', 'studentUniqueId']
        ],
        metaPrefix=None,
        recordPrefix='disciplines_',
        errors='ignore'
    )
    ############################
    # disciplineActions: Staffs
    ############################
    discipline_action_staff_normalized = jsonNormalize(
        discipline_action_content,
        recordPath='staffs',
        meta=[
            'disciplineActionIdentifier',
            'disciplineDate',
            ['studentReference', 'studentUniqueId']
        ],
        metaPrefix=None,
        recordPrefix='staffs_',
        errors='ignore'
    )
    addColumnIfNotExists(discipline_action_staff_normalized, 'staffs_staffReference.staffUniqueId', '')
    ############################
    # Join disciplineAction and Staffs
    ############################
    discipline_action_normalized = pdMerge(
        left=discipline_action_normalized,
        right=discipline_action_staff_normalized,
        how='left',
        leftOn=[
            'disciplineActionIdentifier',
            'disciplineDate',
            'studentReference.studentUniqueId'],
        rightOn=[
            'disciplineActionIdentifier',
            'disciplineDate',
            'studentReference.studentUniqueId'],
        suffixLeft='_discipline_action',
        suffixRight='_staff'
    )
    # Select needed columns.
    discipline_action_normalized = subset(discipline_action_normalized, [
        'disciplineActionIdentifier',
        'disciplineDate',
        'disciplines_disciplineDescriptor',
        'studentReference.studentUniqueId',
        'staffs_staffReference.staffUniqueId'
    ])
    discipline_action_normalized = renameColumns(discipline_action_normalized, {
        'disciplines_disciplineDescriptor': 'disciplineDescriptorCodeValue',
        'studentReference.studentUniqueId': 'studentUniqueId',
        'staffs_staffReference.staffUniqueId': 'userKey'
    })

    discipline_action_normalized['disciplineDateKey'] = to_datetime_key(discipline_action_normalized, 'disciplineDate')
    # Removes namespace
    get_descriptor_code_value_from_uri(discipline_action_normalized, 'disciplineDescriptorCodeValue')

    ############################
    # Discipline descriptor
    ############################
    discipline_descriptor_normalized = jsonNormalize(
        discipline_descriptor_content,
        recordPath=None,
        meta=[
            'description',
            'codeValue'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    discipline_descriptor_normalized = renameColumns(discipline_descriptor_normalized, {
        'description': 'disciplineActionDescription',
        'codeValue': 'disciplineDescriptorCodeValue'
    })
    discipline_action_normalized = pdMerge(
        left=discipline_action_normalized,
        right=discipline_descriptor_normalized,
        how='left',
        leftOn=['disciplineDescriptorCodeValue'],
        rightOn=['disciplineDescriptorCodeValue'],
        suffixLeft='_discipline_action',
        suffixRight='_discipline_descriptor'
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
            'exitWithdrawDate'
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
        'date_now'
    ])
    student_school_association_normalized = renameColumns(student_school_association_normalized, {
        'schoolReference.schoolId': 'schoolId',
        'studentReference.studentUniqueId': 'studentUniqueId'
    })
    ############################
    # discipline_action - student_school_association
    ############################
    result_data_frame = pdMerge(
        left=discipline_action_normalized,
        right=student_school_association_normalized,
        how='inner',
        leftOn=['studentUniqueId'],
        rightOn=['studentUniqueId'],
        suffixLeft='_discipline_action',
        suffixRight='_student_school_association'
    )
    # Filter by exitWithdrawDate
    result_data_frame = result_data_frame[result_data_frame['exitWithdrawDate'] >= result_data_frame['date_now']]
    # Cast as String to concat columns and create keys.
    result_data_frame['disciplineActionIdentifier'] = result_data_frame['disciplineActionIdentifier'].astype(str)
    result_data_frame['studentKey'] = result_data_frame['studentUniqueId'].astype(str)
    result_data_frame['schoolKey'] = result_data_frame['schoolId'].astype(str)
    result_data_frame['disciplineDateKey'] = result_data_frame['disciplineDateKey'].astype(str)
    # Add keys
    result_data_frame['studentDisciplineActionKey'] = (
        result_data_frame['disciplineActionIdentifier']
        + '-'
        + result_data_frame['disciplineDateKey']
        + '-'
        + result_data_frame['studentKey']
        + '-'
        + result_data_frame['schoolKey']
    )
    result_data_frame['studentSchoolKey'] = (
        result_data_frame['studentKey']
        + '-'
        + result_data_frame['schoolKey']
    )
    # Select needed columns.
    result_data_frame = subset(result_data_frame, [
        'studentDisciplineActionKey',
        'studentSchoolKey',
        'disciplineDateKey',
        'studentKey',
        'schoolKey',
        'disciplineActionDescription',
        'userKey'
    ])

    saveParquetFile(result_data_frame, f"{config('PARQUET_FILES_LOCATION')}", "equity_StudentDisciplineActionDim.parquet", school_year)

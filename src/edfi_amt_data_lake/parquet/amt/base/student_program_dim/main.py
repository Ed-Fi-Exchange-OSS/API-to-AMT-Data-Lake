
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
    get_reference_from_href,
    is_data_frame_empty,
    jsonNormalize,
    pdMerge,
    renameColumns,
    subset,
    to_datetime_key,
)

ENDPOINT_PROGRAMS = 'programs'
ENDPOINT_STUDENT_SCHOOL_ASSOCIATION = "studentSchoolAssociations"
ENDPOINT_STUDENT_PROGRAM_ASSOCIATIONS = 'studentProgramAssociations'
ENDPOINT_PROGRAM_TYPE_DESCRIPTOR = 'programTypeDescriptors'
RESULT_COLUMNS = [
    'StudentSchoolProgramKey',
    'BeginDateKey',
    'EducationOrganizationId',
    'ProgramName',
    'StudentKey',
    'SchoolKey',
    'StudentSchoolKey',
    'EducationOrganizationKey'
]


@create_parquet_file
def student_program_dim_frame(
    file_name: str,
    columns: list[str],
    school_year: int
):
    file_name = file_name
    programs_json = getEndpointJson(ENDPOINT_PROGRAMS, config('SILVER_DATA_LOCATION'), school_year)
    student_program_dim_json = getEndpointJson(ENDPOINT_STUDENT_PROGRAM_ASSOCIATIONS, config('SILVER_DATA_LOCATION'), school_year)
    student_school_association_json = getEndpointJson(ENDPOINT_STUDENT_SCHOOL_ASSOCIATION, config('SILVER_DATA_LOCATION'), school_year)
    program_type_descriptor_json = getEndpointJson(ENDPOINT_PROGRAM_TYPE_DESCRIPTOR, config('SILVER_DATA_LOCATION'), school_year)
    program_type_descriptor = jsonNormalize(
        program_type_descriptor_json,
        recordPath=None,
        meta=[
            'programTypeDescriptorId',
            'codeValue',
            'description',
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors="ignore",
    )
    program_type_descriptor = renameColumns(
        program_type_descriptor,
        {
            'codeValue': 'programTypeDescriptor',
            'description': 'programTypeDescriptorDescription'
        }
    )
    student_program_dim = jsonNormalize(
        student_program_dim_json,
        recordPath=None,
        meta=[
            'studentReference.studentUniqueId',
            'beginDate',
            'programReference.programName',
            'programReference.programTypeDescriptor',
            'programReference.educationOrganizationId',
            'programReference.link.href',
            'educationOrganizationReference.educationOrganizationId'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors="ignore",
    )
    get_reference_from_href(
        student_program_dim,
        'programReference.link.href',
        'programReferenceId'
    )
    get_descriptor_code_value_from_uri(student_program_dim, 'programReference.programTypeDescriptor')
    student_program_dim = pdMerge(
        left=student_program_dim,
        right=program_type_descriptor,
        how='inner',
        leftOn=['programReference.programTypeDescriptor'],
        rightOn=['programTypeDescriptor'],
        suffixLeft=None,
        suffixRight=None,
    )
    if is_data_frame_empty(student_program_dim):
        return None
    student_program_dim = subset(
        student_program_dim,
        [
            'studentReference.studentUniqueId',
            'beginDate',
            'programReference.programName',
            'programReference.programTypeDescriptor',
            'programReference.educationOrganizationId',
            'programReferenceId',
            'educationOrganizationReference.educationOrganizationId',
            'programTypeDescriptorId'
        ]
    )
    student_school_association = jsonNormalize(
        student_school_association_json,
        recordPath=None,
        meta=[
            'studentReference.studentUniqueId',
            'schoolReference.schoolId',
            'graduationPlanReference.educationOrganizationId',
            'exitWithdrawDate'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors="ignore",
    )
    student_school_association['exitWithdrawDateKey'] = to_datetime_key(student_school_association, 'exitWithdrawDate')
    student_school_association['dateKey'] = date.today().strftime('%Y%m%d')
    programs = jsonNormalize(
        programs_json,
        recordPath=None,
        meta=[
            'id',
            'programTypeDescriptor',
            'programName',
            'educationOrganizationReference.educationOrganizationId'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors="ignore",
    )
    programs = renameColumns(
        programs,
        {
            'id': 'programReferenceId'
        }
    )
    prev_data_frame = pdMerge(
        left=student_program_dim,
        right=student_school_association,
        how='inner',
        leftOn=['studentReference.studentUniqueId'],
        rightOn=['studentReference.studentUniqueId'],
        suffixLeft="_left",
        suffixRight=None,
    )
    if is_data_frame_empty(prev_data_frame):
        return None
    data_frame = pdMerge(
        left=prev_data_frame,
        right=programs,
        how='inner',
        leftOn=[
            'programReferenceId',
            'programReference.educationOrganizationId'
        ],
        rightOn=[
            'programReferenceId',
            'educationOrganizationReference.educationOrganizationId'
        ],
        suffixLeft="_prev_left",
        suffixRight=None,
    )
    if is_data_frame_empty(data_frame):
        return None

    data_frame = subset(data_frame, [
        'programReferenceId',
        'beginDate',
        'educationOrganizationReference.educationOrganizationId_prev_left',
        'programReference.educationOrganizationId',
        'programReference.programTypeDescriptor',
        'studentReference.studentUniqueId',
        'schoolReference.schoolId',
        'graduationPlanReference.educationOrganizationId',
        'programReference.programName',
        'programTypeDescriptorId',
        'exitWithdrawDateKey',
        'dateKey'
    ])
    data_frame = (
        data_frame[
            data_frame['exitWithdrawDateKey'] >= data_frame['dateKey']
        ]
    )
    data_frame['beginDate'] = to_datetime_key(data_frame, 'beginDate')

    data_frame['StudentSchoolProgramKey'] = (
        data_frame['studentReference.studentUniqueId'].astype(str) + '-'
        + data_frame['schoolReference.schoolId'].astype(str) + '-'
        + data_frame['programReference.programName'].astype(str) + '-'
        + data_frame['programTypeDescriptorId'].astype(str) + '-'
        + data_frame['educationOrganizationReference.educationOrganizationId_prev_left'].astype(str) + '-'
        + data_frame['programReference.educationOrganizationId'].astype(str) + '-'
        + data_frame['beginDate']
    )
    data_frame['StudentSchoolKey'] = (
        data_frame['studentReference.studentUniqueId'].astype(str) + '-'
        + data_frame['schoolReference.schoolId'].astype(str)
    )

    data_frame['EducationOrganizationKey'] = (
        data_frame['educationOrganizationReference.educationOrganizationId_prev_left'].astype(str)
    )

    data_frame['schoolReference.schoolId'] = data_frame['schoolReference.schoolId'].astype(str)
    data_frame['programReference.educationOrganizationId'] = data_frame['programReference.educationOrganizationId'].astype(str)

    data_frame = renameColumns(data_frame, {
        'beginDate': 'BeginDateKey',
        'programReference.educationOrganizationId': 'EducationOrganizationId',
        'schoolReference.schoolId': 'SchoolKey',
        'programReference.programName': 'ProgramName',
        'studentReference.studentUniqueId': 'StudentKey',
    })
    data_frame = data_frame[columns]
    return data_frame


def student_program_dim(school_year) -> data_frame_generation_result:
    return student_program_dim_frame(
        file_name="studentProgramDim.parquet",
        columns=RESULT_COLUMNS,
        school_year=school_year
    )

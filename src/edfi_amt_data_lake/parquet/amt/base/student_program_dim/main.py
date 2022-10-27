
# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

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

ENDPOINT_PROGRAMS = 'programs'
ENDPOINT_STUDENT_SCHOOL_ASSOCIATION = "studentSchoolAssociations"
ENDPOINT_STUDENT_PROGRAM_ASSOCIATIONS = 'studentProgramAssociations'


def student_program_dim(school_year: str) -> None:

    programs_json = getEndpointJson(ENDPOINT_PROGRAMS, config('SILVER_DATA_LOCATION'), school_year)
    student_program_dim_json = getEndpointJson(ENDPOINT_STUDENT_PROGRAM_ASSOCIATIONS, config('SILVER_DATA_LOCATION'), school_year)
    student_school_association_json = getEndpointJson(ENDPOINT_STUDENT_SCHOOL_ASSOCIATION, config('SILVER_DATA_LOCATION'), school_year)

    student_program_dim = jsonNormalize(
        student_program_dim_json,
        recordPath=None,
        meta=[],
        metaPrefix=None,
        recordPrefix=None,
        errors="ignore",
    )

    student_school_association = jsonNormalize(
        student_school_association_json,
        recordPath=None,
        meta=[],
        metaPrefix=None,
        recordPrefix=None,
        errors="ignore",
    )

    programs = jsonNormalize(
        programs_json,
        recordPath=None,
        meta=[],
        metaPrefix=None,
        recordPrefix=None,
        errors="ignore",
    )

    data_frame = pdMerge(
        left=student_program_dim,
        right=student_school_association,
        how='left',
        leftOn=['studentReference.studentUniqueId'],
        rigthOn=['studentReference.studentUniqueId'],
        suffixLeft="_left",
        suffixRight=None,
    )

    data_frame = pdMerge(
        left=programs,
        right=data_frame,
        how='right',
        leftOn=['programTypeDescriptor'],
        rigthOn=['programReference.programTypeDescriptor'],
        suffixLeft="_dt_left",
        suffixRight="_dt_right",
    )

    get_descriptor_code_value_from_uri(data_frame, 'programReference.programTypeDescriptor')

    data_frame = subset(data_frame, [
        'studentReference.studentUniqueId',
        'schoolReference.schoolId',
        'programReference.programName',
        'programReference.programTypeDescriptor',
        'programReference.educationOrganizationId',
        'beginDate',
        'programId',
        'graduationPlanReference.educationOrganizationId',
    ])

    data_frame['LastModifiedDate'] = ''

    data_frame['beginDate'] = to_datetime_key(data_frame, 'beginDate')

    data_frame['StudentSchoolProgramKey'] = (
        data_frame['studentReference.studentUniqueId'].astype(str) + '-'
        + data_frame['schoolReference.schoolId'].astype(str) + '-'
        + data_frame['programReference.programName'].astype(str) + '-'
        + data_frame['programId'].astype(str) + '-'
        + data_frame['programReference.educationOrganizationId'].astype(str) + '-'
        + data_frame['programReference.educationOrganizationId'].astype(str) + '-'
        + data_frame['beginDate']
    )

    data_frame['StudentSchoolKey'] = (
        data_frame['studentReference.studentUniqueId'].astype(str) + '-'
        + data_frame['schoolReference.schoolId'].astype(str)
    )

    data_frame = renameColumns(data_frame, {
        'beginDate': 'BeginDateKey',
        'programReference.educationOrganizationId': 'EducationOrganizationId',
        'schoolReference.schoolId': 'SchoolKey',
        'programReference.programName': 'ProgramName',
        'studentReference.studentUniqueId': 'StudentKey',
    })

    data_frame = data_frame[[
        'StudentSchoolProgramKey',
        'BeginDateKey',
        'EducationOrganizationId',
        'ProgramName',
        'StudentKey',
        'SchoolKey',
        'StudentSchoolKey',
        'LastModifiedDate'
    ]]

    saveParquetFile(data_frame, f"{config('PARQUET_FILES_LOCATION')}", "equity_StudentProgramFact.parquet", school_year)

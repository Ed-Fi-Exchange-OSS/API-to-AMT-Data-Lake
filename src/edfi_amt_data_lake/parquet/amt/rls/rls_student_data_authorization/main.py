# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from decouple import config

from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    addColumnIfNotExists,
    create_parquet_file,
    get_reference_from_href,
    jsonNormalize,
    renameColumns,
    subset,
)

ENDPOINT_SECTION_ASSOCIATION = 'studentSectionAssociations'
RESULT_COLUMNS = [
    'StudentKey',
    'SchoolKey',
    'SectionId',
    'BeginDate',
    'EndDate'
]


@create_parquet_file
def rls_student_data_authorization_dataframe(
    file_name: str,
    columns: list[str],
    school_year: int
):
    file_name = file_name
    student_section_association_content = getEndpointJson(ENDPOINT_SECTION_ASSOCIATION, config('SILVER_DATA_LOCATION'), school_year)
    ############################
    # studentSectionAssociations
    ############################
    student_section_association_normalize = jsonNormalize(
        student_section_association_content,
        recordPath=None,
        meta=[
            'studentReference.studentUniqueId',
            'sectionReference.schoolId',
            'sectionReference.link.href',
            'beginDate',
            'endDate'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    student_section_association_normalize = renameColumns(student_section_association_normalize, {
        'studentReference.studentUniqueId': 'studentKey',
        'sectionReference.schoolId': 'schoolKey',
    })
    addColumnIfNotExists(student_section_association_normalize, 'endDate')
    get_reference_from_href(student_section_association_normalize, 'sectionReference.link.href', 'sectionId')
    # Select needed columns.
    student_section_association_normalize = renameColumns(
        student_section_association_normalize,
        {
            'studentKey': 'StudentKey',
            'schoolKey': 'SchoolKey',
            'sectionId': 'SectionId',
            'beginDate': 'BeginDate',
            'endDate': 'EndDate'
        }
    )
    # Select needed columns.
    result_data_frame = subset(student_section_association_normalize, columns)
    return result_data_frame


def rls_student_data_authorization(school_year) -> None:
    return rls_student_data_authorization_dataframe(
        file_name="rls_StudentDataAuthorization.parquet",
        columns=RESULT_COLUMNS,
        school_year=school_year
    )

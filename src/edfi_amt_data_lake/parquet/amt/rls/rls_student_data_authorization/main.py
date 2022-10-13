# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import pandas as pd
from decouple import config

from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    jsonNormalize,
    renameColumns,
    saveParquetFile,
    subset,
    get_reference_from_href,toCsv
)

ENDPOINT_SECTION_ASSOCIATION = 'studentSectionAssociations'
ENDPOINT_SECTION = 'sections'


def rls_student_data_authorization_dataframe(school_year) -> pd.DataFrame:
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
    get_reference_from_href(student_section_association_normalize, 'id', 'sectionId')
    # Select needed columns.
    result_data_frame = subset(student_section_association_normalize, [
        'studentKey',
        'schoolKey',
        'sectionId',
        'beginDate',
        'endDate'
    ])
    return result_data_frame


def rls_student_data_authorization(school_year) -> None:
    result_data_frame = rls_student_data_authorization_dataframe(school_year)
    saveParquetFile(result_data_frame, f"{config('PARQUET_FILES_LOCATION')}", "rls_StudentDataAuthorization.parquet", school_year)

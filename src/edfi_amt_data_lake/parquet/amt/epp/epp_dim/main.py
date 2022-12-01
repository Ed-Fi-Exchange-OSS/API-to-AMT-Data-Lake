# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from decouple import config

from edfi_amt_data_lake.helper.data_frame_generation_result import (
    data_frame_generation_result,
)
from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    create_parquet_file,
    get_descriptor_code_value_from_uri,
    jsonNormalize,
    pdMerge,
    renameColumns,
)

ENDPOINT_SCHOOLS = 'schools'
RESULT_COLUMNS = [
    'EducationOrganizationKey',
    'NameOfInstitution'
]


@create_parquet_file
def epp_dim_dataframe(
    file_name: str,
    columns: list[str],
    school_year: int
):
    file_name = file_name
    epp_content = getEndpointJson(ENDPOINT_SCHOOLS, config('SILVER_DATA_LOCATION'), school_year)

    epp_normalize = jsonNormalize(
        epp_content,
        recordPath=None,
        meta=[
            'schoolId',
            'nameOfInstitution'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    epp_categories_normalize = jsonNormalize(
        epp_content,
        recordPath=['educationOrganizationCategories'],
        meta=[
            'schoolId'
        ],
        recordMeta=[
            'educationOrganizationCategoryDescriptor'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    result_data_frame = pdMerge(
        left=epp_normalize,
        right=epp_categories_normalize,
        how='left',
        leftOn=['schoolId'],
        rightOn=['schoolId'],
        suffixLeft=None,
        suffixRight='_categories'
    )
    if result_data_frame is None:
        return None
    get_descriptor_code_value_from_uri(result_data_frame, 'educationOrganizationCategoryDescriptor')

    result_data_frame = result_data_frame[result_data_frame['educationOrganizationCategoryDescriptor'].str.contains("Preparation Provider", na=False)]

    result_data_frame = result_data_frame[[
        "schoolId",
        "nameOfInstitution"
    ]]

    result_data_frame = renameColumns(result_data_frame, {
        'schoolId': 'EducationOrganizationKey',
        'nameOfInstitution': 'NameOfInstitution'
    })
    result_data_frame = result_data_frame[columns]
    return result_data_frame


def epp_dim(school_year) -> data_frame_generation_result:
    return epp_dim_dataframe(
        file_name="epp_EppDim.parquet",
        columns=RESULT_COLUMNS,
        school_year=school_year
    )

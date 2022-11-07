# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import pandas as pd
from decouple import config

from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    get_descriptor_code_value_from_uri,
    jsonNormalize,
    pdMerge,
    renameColumns,
    saveParquetFile,
)

ENDPOINT_SCHOOLS = 'schools'


def epp_dim_dataframe(school_year) -> pd.DataFrame:
    epp_content = getEndpointJson(ENDPOINT_SCHOOLS, config('SILVER_DATA_LOCATION'), school_year)

    epp_normalize = jsonNormalize(
        epp_content,
        recordPath=None,
        meta=None,
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

    get_descriptor_code_value_from_uri(result_data_frame, 'educationOrganizationCategoryDescriptor')

    result_data_frame = result_data_frame[result_data_frame['educationOrganizationCategoryDescriptor'].str.contains("Preparation Provider")]

    result_data_frame = result_data_frame[[
        "schoolId",
        "nameOfInstitution"
    ]]

    result_data_frame = renameColumns(result_data_frame, {
        'schoolId': 'EducationOrganizationKey',
        'nameOfInstitution': 'NameOfInstitution'
    })

    return result_data_frame


def epp_dim(school_year) -> None:
    result_data_frame = epp_dim_dataframe(school_year)
    saveParquetFile(result_data_frame, f"{config('PARQUET_FILES_LOCATION')}", "epp_EppDim.parquet", school_year)

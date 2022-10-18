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
)

ENDPOINT_SEX_DESCRIPTOR = 'sexDescriptors'


def sex_descriptor_dim_dataframe(school_year) -> pd.DataFrame:
    sex_descriptor_content = getEndpointJson(ENDPOINT_SEX_DESCRIPTOR, config('SILVER_DATA_LOCATION'), school_year)

    sex_descriptor_normalize = jsonNormalize(
        sex_descriptor_content,
        recordPath=None,
        meta=[
            'sexDescriptorId',
            'codeValue',
            'description'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    result_data_frame = renameColumns(sex_descriptor_normalize, {
        'sexDescriptorId': 'SexDescriptorKey',
        'codeValue': 'CodeValue'
    })

    result_data_frame = result_data_frame[[
        "SexDescriptorKey",
        "CodeValue"
    ]]

    return result_data_frame


def sex_descriptor_dim(school_year) -> None:
    result_data_frame = sex_descriptor_dim_dataframe(school_year)
    saveParquetFile(result_data_frame, f"{config('PARQUET_FILES_LOCATION')}", "epp_SexDescriptorDim.parquet", school_year)

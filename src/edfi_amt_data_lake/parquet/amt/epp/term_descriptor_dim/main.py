# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import pandas as pd
from decouple import config
from edfi_amt_data_lake.helper.data_frame_generation_result import (
    data_frame_generation_result,
)
from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    create_parquet_file,
    jsonNormalize,
    renameColumns,
    toCsv,
)
ENDPOINT_TERM_DESCRIPTOR = 'termDescriptors'
RESULT_COLUMNS = [
    "TermDescriptorKey",
    "CodeValue"
]


@create_parquet_file
def term_descriptor_dim_dataframe(
    file_name: str,
    columns: list[str],
    school_year: int
):
    file_name = file_name
    term_descriptor_content = getEndpointJson(ENDPOINT_TERM_DESCRIPTOR, config('SILVER_DATA_LOCATION'), school_year)

    term_descriptor_normalize = jsonNormalize(
        term_descriptor_content,
        recordPath=None,
        meta=[
            'termDescriptorId',
            'codeValue',
            'description'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    result_data_frame = renameColumns(term_descriptor_normalize, {
        'termDescriptorId': 'TermDescriptorKey',
        'codeValue': 'CodeValue'
    })
    result_data_frame = result_data_frame[columns]
    return result_data_frame


def term_descriptor_dim(school_year) -> data_frame_generation_result:
    return term_descriptor_dim_dataframe(
        file_name="epp_TermDescriptorDim.parquet",
        columns=RESULT_COLUMNS,
        school_year=school_year
    )

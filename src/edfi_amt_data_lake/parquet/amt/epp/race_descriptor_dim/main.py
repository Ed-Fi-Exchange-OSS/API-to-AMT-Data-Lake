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
)
ENDPOINT_RACE_DESCRIPTOR = 'raceDescriptors'
RESULT_COLUMNS = [
    "RaceDescriptorKey",
    "CodeValue"
]


@create_parquet_file
def race_descriptor_dim_dataframe(
    file_name: str,
    columns: list[str],
    school_year: int
):
    file_name = file_name
    race_descriptor_content = getEndpointJson(ENDPOINT_RACE_DESCRIPTOR, config('SILVER_DATA_LOCATION'), school_year)

    race_descriptor_normalize = jsonNormalize(
        race_descriptor_content,
        recordPath=None,
        meta=[
            'raceDescriptorId',
            'codeValue',
            'description'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    result_data_frame = renameColumns(race_descriptor_normalize, {
        'raceDescriptorId': 'RaceDescriptorKey',
        'codeValue': 'CodeValue'
    })
    result_data_frame = result_data_frame[columns]
    return result_data_frame


def race_descriptor_dim(school_year) -> data_frame_generation_result:
    return race_descriptor_dim_dataframe(
        file_name="epp_RaceDescriptorDim.parquet",
        columns=RESULT_COLUMNS,
        school_year=school_year
    )
    

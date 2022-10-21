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

ENDPOINT_RACE_DESCRIPTOR = 'raceDescriptors'


def race_descriptor_dim_dataframe(school_year) -> pd.DataFrame:
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

    result_data_frame = result_data_frame[[
        "RaceDescriptorKey",
        "CodeValue"
    ]]

    return result_data_frame


def race_descriptor_dim(school_year) -> None:
    result_data_frame = race_descriptor_dim_dataframe(school_year)
    saveParquetFile(result_data_frame, f"{config('PARQUET_FILES_LOCATION')}", "epp_RaceDescriptorDim.parquet", school_year)

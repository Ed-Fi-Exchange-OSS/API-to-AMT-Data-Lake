# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import pandas as pd

from edfi_amt_data_lake.helper.helper import get_descriptor_mapping_config
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    jsonNormalize,
    pdMerge,
    renameColumns,
)


def get_descriptor_constant(data=pd.DataFrame, column=str):
    if column in data:
        descriptor_mapping_content = get_descriptor_mapping_config()
        ############################
        # descriptor_mapping
        ############################
        descriptor_mapping_normalized = jsonNormalize(
            descriptor_mapping_content
            , recordPath=None
            , meta=['constantName', 'descriptor', 'codeValue']
            , metaPrefix=None
            , recordPrefix=None
            , errors='ignore'
        )
        descriptor_mapping_normalized = renameColumns(descriptor_mapping_normalized, {
            'codeValue': f"{column}_codeValue",
            'constantName': f"{column}_constantName",
            'descriptor': f"{column}_descriptor"
        })

        if not data[column].empty:
            if len(data[f"{column}"].str.split('#')) > 0:
                data[f"{column}_descriptor"] = column
                data[f"{column}_codeValue"] = data[column].str.split("#").str.get(-1)

        ############################
        # Join to get descriptor constant
        ############################
        data = pdMerge(
            left=descriptor_mapping_normalized,
            right=data,
            how='right',
            leftOn=[
                descriptor_mapping_normalized[f"{column}_descriptor"].str.lower(),
                descriptor_mapping_normalized[f"{column}_codeValue"].str.lower()
            ],
            rightOn=[
                data[f"{column}_descriptor"].str.lower(),
                data[f"{column}_codeValue"].str.lower()
            ],
            suffixLeft='_descriptor_mapping_normalized',
            suffixRight='_data'
        )
    else:
        data[f"{column}_descriptor"] = ""
        data[f"{column}_codeValue"] = ""
        data[f"{column}_constantName"] = ""
    return data

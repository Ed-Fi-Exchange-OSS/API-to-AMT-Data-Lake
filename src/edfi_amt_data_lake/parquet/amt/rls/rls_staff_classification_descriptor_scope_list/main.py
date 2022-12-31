# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import pandas as pd
from decouple import config

from edfi_amt_data_lake.helper.helper import get_descriptor_mapping_config
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    jsonNormalize,
    renameColumns,
    saveParquetFile,
)


def rls_staff_classification_descriptor_scope_list_dataframe(school_year) -> pd.DataFrame:

    descriptor_mapping_content = get_descriptor_mapping_config()

    descriptor_mapping_normalized = jsonNormalize(
        descriptor_mapping_content
        , recordPath=None
        , meta=['constantName', 'codeValue']
        , metaPrefix=None
        , recordPrefix=None
        , errors='ignore'
    )

    result_data_frame = (
        descriptor_mapping_normalized[
            (descriptor_mapping_normalized['constantName'].str.contains('AuthorizationScope.District', na=False))
            | (descriptor_mapping_normalized['constantName'].str.contains('AuthorizationScope.School', na=False))
            | (descriptor_mapping_normalized['constantName'].str.contains('AuthorizationScope.Section', na=False))
        ]
    )

    result_data_frame = result_data_frame[['constantName', 'codeValue']]

    result_data_frame = renameColumns(result_data_frame, {
        'constantName': "AuthorizationScopeName",
        'codeValue': "CodeValue"
    })

    return result_data_frame


def rls_staff_classification_descriptor_scope_list(school_year) -> None:
    result_data_frame = rls_staff_classification_descriptor_scope_list_dataframe(school_year)
    saveParquetFile(result_data_frame, f"{config('PARQUET_FILES_LOCATION')}", "rls_StaffClassificationDescriptorScopeList.parquet", school_year)

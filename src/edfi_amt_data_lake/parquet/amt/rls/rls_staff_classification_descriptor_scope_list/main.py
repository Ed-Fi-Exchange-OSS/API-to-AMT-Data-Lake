# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import pandas as pd

from edfi_amt_data_lake.helper.data_frame_generation_result import (
    data_frame_generation_result,
)
from edfi_amt_data_lake.helper.helper import get_descriptor_mapping_config
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    create_empty_data_frame,
    create_parquet_file,
    jsonNormalize,
    renameColumns,
)

RESULT_COLUMNS = [
    'AuthorizationScopeName',
    'CodeValue'
]


@create_parquet_file
def rls_staff_classification_descriptor_scope_list_dataframe(
    file_name: str,
    columns: list[str],
    school_year: int
) -> pd.DataFrame:

    descriptor_mapping_content = get_descriptor_mapping_config()

    descriptor_mapping_normalized = jsonNormalize(
        descriptor_mapping_content
        , recordPath=None
        , meta=['constantName', 'codeValue']
        , metaPrefix=None
        , recordPrefix=None
        , errors='ignore'
    )

    if descriptor_mapping_normalized.empty:
        return create_empty_data_frame(columns)

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

    return result_data_frame[columns]


def rls_staff_classification_descriptor_scope_list(school_year) -> data_frame_generation_result:
    return rls_staff_classification_descriptor_scope_list_dataframe(
        file_name="rls_StaffClassificationDescriptorScopeList.parquet",
        columns=RESULT_COLUMNS,
        school_year=school_year
    )

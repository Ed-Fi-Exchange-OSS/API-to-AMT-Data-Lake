# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import pandas as pd
from decouple import config

from edfi_amt_data_lake.parquet.Common.descriptor_mapping import get_descriptor_constant
from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    jsonNormalize,
    pdMerge,
    renameColumns,
    saveParquetFile,
)

ENDPOINT_STAFFS = 'staffs'


def rls_user_dim_dataframe(school_year) -> pd.DataFrame:
    staffs_content = getEndpointJson(ENDPOINT_STAFFS, config('SILVER_DATA_LOCATION'), school_year)

    staffs_normalize = jsonNormalize(
        staffs_content,
        recordPath=None,
        meta=[
            'staffUniqueId'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    staffs_emails_normalize = jsonNormalize(
        staffs_content,
        recordPath=['electronicMails'],
        meta=[
            'staffUniqueId'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    result_data_frame = pdMerge(
        left=staffs_normalize,
        right=staffs_emails_normalize,
        how='inner',
        leftOn=[
            'staffUniqueId'
        ],
        rightOn=[
            'staffUniqueId'
        ],
        suffixLeft=None,
        suffixRight='_emails'
    )

    result_data_frame = get_descriptor_constant(result_data_frame, 'electronicMailTypeDescriptor')
    result_data_frame = result_data_frame[result_data_frame["electronicMailTypeDescriptor_constantName"].str.contains('Email.Work', na=False)]

    result_data_frame = renameColumns(result_data_frame, {
        'staffUniqueId': 'UserKey',
        'electronicMailAddress': 'UserEmail'
    })

    result_data_frame = result_data_frame[[
        'UserKey',
        'UserEmail'
    ]]

    return result_data_frame


def rls_user_dim(school_year) -> None:
    result_data_frame = rls_user_dim_dataframe(school_year)
    saveParquetFile(result_data_frame, f"{config('PARQUET_FILES_LOCATION')}", "rls_user_dim.parquet", school_year)

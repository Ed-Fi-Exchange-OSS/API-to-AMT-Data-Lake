# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from decouple import config

from edfi_amt_data_lake.parquet.Common.descriptor_mapping import get_descriptor_constant
from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    addColumnIfNotExists,
    create_parquet_file,
    jsonNormalize,
    pdMerge,
    renameColumns,
    subset,
)

ENDPOINT_STAFFS = 'staffs'
RESULT_COLUMNS = [
    'UserKey',
    'UserEmail'
]


@create_parquet_file
def rls_user_dim_dataframe(
    file_name: str,
    columns: list[str],
    school_year: int
):
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

    if staffs_normalize.empty:
        return None

    staffs_emails_normalize = jsonNormalize(
        staffs_content,
        recordPath=['electronicMails'],
        meta=[
            'staffUniqueId'
        ],
        recordMeta=[
            'electronicMailAddress',
            'electronicMailTypeDescriptor'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    if not staffs_emails_normalize.empty:
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

    addColumnIfNotExists(result_data_frame, 'electronicMailAddress')

    result_data_frame = renameColumns(result_data_frame, {
        'staffUniqueId': 'UserKey',
        'electronicMailAddress': 'UserEmail'
    })

    result_data_frame = subset(result_data_frame, columns)

    return result_data_frame


def rls_user_dim(school_year) -> None:
    return rls_user_dim_dataframe(
        file_name="rls_user_dim.parquet",
        columns=RESULT_COLUMNS,
        school_year=school_year
    )

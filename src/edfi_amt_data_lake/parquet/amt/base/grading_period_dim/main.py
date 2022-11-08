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
    subset,
    to_datetime_key,
)

ENDPOINT_GRADING_PERIOD = 'gradingPeriods'
ENDPOINT_GRADING_PERIOD_DESCRIPTOR = 'gradingPeriodDescriptors'


def grading_period_dim_dataframe(school_year) -> pd.DataFrame:
    grading_period_content = getEndpointJson(ENDPOINT_GRADING_PERIOD, config('SILVER_DATA_LOCATION'), school_year)
    grading_period_descriptor_content = getEndpointJson(ENDPOINT_GRADING_PERIOD_DESCRIPTOR, config('SILVER_DATA_LOCATION'), school_year)
    ############################
    # gradingPeriods
    ############################
    grading_period_normalize = jsonNormalize(
        grading_period_content,
        recordPath=None,
        meta=[
            'schoolReference.schoolId',
            'beginDate',
            'endDate',
            'gradingPeriodDescriptor',
            'totalInstructionalDays',
            'periodSequence',
            'schoolYearTypeReference.schoolYear'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    get_descriptor_code_value_from_uri(grading_period_normalize, 'gradingPeriodDescriptor')
    grading_period_normalize = renameColumns(grading_period_normalize, {
        'schoolReference.schoolId': 'schoolId',
        'schoolYearTypeReference.schoolYear': 'schoolYear',
        'gradingPeriodDescriptor': 'gradingPeriodDescriptorCodeValue'
    })

    # Select needed columns.
    grading_period_normalize = subset(grading_period_normalize, [
        'schoolId',
        'beginDate',
        'endDate',
        'gradingPeriodDescriptorCodeValue',
        'totalInstructionalDays',
        'periodSequence',
        'schoolYear'
    ])

    ############################
    # gradingPeriodDescriptors
    ############################
    grading_period_descriptor_normalize = jsonNormalize(
        grading_period_descriptor_content,
        recordPath=None,
        meta=[
            'gradingPeriodDescriptorId',
            'codeValue'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    grading_period_descriptor_normalize = renameColumns(grading_period_descriptor_normalize, {
        'codeValue': 'gradingPeriodDescriptorCodeValue'
    })
    # Select needed columns.
    grading_period_descriptor_normalize = subset(grading_period_descriptor_normalize, [
        'gradingPeriodDescriptorId',
        'gradingPeriodDescriptorCodeValue'
    ])
    ############################
    # Merge grading_period - gradingPeriodDescriptors
    ############################
    result_data_frame = pdMerge(
        left=grading_period_normalize,
        right=grading_period_descriptor_normalize,
        how='inner',
        leftOn=['gradingPeriodDescriptorCodeValue'],
        rightOn=['gradingPeriodDescriptorCodeValue'],
        suffixLeft='_grading_period',
        suffixRight='_grading_period_descriptor'
    )
    result_data_frame['gradingPeriodDescriptorId'] = result_data_frame['gradingPeriodDescriptorId'].astype(str)
    result_data_frame['schoolKey'] = result_data_frame['schoolId'].astype(str)
    result_data_frame['schoolYear'] = result_data_frame['schoolYear'].astype(str)
    result_data_frame['gradingPeriodBeginDateKey'] = to_datetime_key(result_data_frame, 'beginDate')
    result_data_frame['gradingPeriodEndDateKey'] = to_datetime_key(result_data_frame, 'endDate')
    result_data_frame['gradingPeriodDescription'] = result_data_frame['gradingPeriodDescriptorCodeValue']
    result_data_frame['gradingPeriodKey'] = (
        result_data_frame['gradingPeriodDescriptorId']
        + '-' + result_data_frame['schoolKey']
        + '-' + result_data_frame['gradingPeriodBeginDateKey']
    )

    # Select needed columns.
    result_data_frame = subset(result_data_frame, [
        'gradingPeriodKey',
        'gradingPeriodBeginDateKey',
        'gradingPeriodEndDateKey',
        'gradingPeriodDescription',
        'totalInstructionalDays',
        'periodSequence',
        'schoolKey',
        'schoolYear'
    ])

    return result_data_frame


def grading_period_dim(school_year) -> None:
    result_data_frame = grading_period_dim_dataframe(school_year)

    saveParquetFile(result_data_frame, f"{config('PARQUET_FILES_LOCATION')}", "gradingPeriodDim.parquet", school_year)

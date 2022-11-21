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
    addColumnIfNotExists,
    create_empty_data_frame,
    create_parquet_file,
    jsonNormalize,
    pdMerge,
    renameColumns,
)

ENDPOINT_SECTIONS = 'sections'
RESULT_COLUMNS = [
    'ClassPeriodKey',
    'SectionKey',
    'ClassPeriodName',
    'LocalCourseCode',
    'SchoolId',
    'SchoolYear',
    'SectionIdentifier',
    'SessionName'
]


@create_parquet_file
def class_period_dim_data_frame(
    file_name: str,
    columns: list[str],
    school_year: int
) -> pd.DataFrame:
    file_name = file_name
    sections_content = getEndpointJson(ENDPOINT_SECTIONS, config('SILVER_DATA_LOCATION'), school_year)

    schools_normalized = jsonNormalize(
        data=sections_content,
        recordPath=None,
        meta=[
            'id',
            'sectionIdentifier',
            ['courseOfferingReference', 'localCourseCode'],
            ['courseOfferingReference', 'schoolId'],
            ['courseOfferingReference', 'schoolYear'],
            ['courseOfferingReference', 'sessionName']
        ],
        recordMeta=None,
        metaPrefix=None,
        recordPrefix='',
        errors='ignore'
    )

    if schools_normalized.empty:
        return create_empty_data_frame(columns=columns)

    sections_class_periods = jsonNormalize(
        data=sections_content,
        recordPath=['classPeriods'],
        meta=[
            'id'
        ],
        recordMeta=[
            ['classPeriodReference', 'classPeriodName']
        ],
        metaPrefix=None,
        recordPrefix='',
        errors='ignore'
    )

    result_data_frame = pdMerge(
        left=schools_normalized,
        right=sections_class_periods,
        how='left',
        leftOn=['id'],
        rightOn=['id'],
        suffixLeft='',
        suffixRight='_class_periods'
    )

    addColumnIfNotExists(result_data_frame, 'classPeriodReference.classPeriodName')

    result_data_frame["ClassPeriodKey"] = (
        result_data_frame["classPeriodReference.classPeriodName"] + "-"
        + result_data_frame["courseOfferingReference.localCourseCode"] + "-"
        + result_data_frame["courseOfferingReference.schoolId"].astype(str) + "-"
        + result_data_frame["courseOfferingReference.schoolYear"].astype(str) + "-"
        + result_data_frame["sectionIdentifier"] + "-"
        + result_data_frame["courseOfferingReference.sessionName"]
    )

    result_data_frame["SectionKey"] = (
        result_data_frame["courseOfferingReference.schoolId"].astype(str) + "-"
        + result_data_frame["courseOfferingReference.localCourseCode"] + "-"
        + result_data_frame["courseOfferingReference.schoolYear"].astype(str) + "-"
        + result_data_frame["sectionIdentifier"] + "-"
        + result_data_frame["courseOfferingReference.sessionName"]
    )

    result_data_frame = renameColumns(result_data_frame, {
        'classPeriodReference.classPeriodName': 'ClassPeriodName',
        'courseOfferingReference.localCourseCode': 'LocalCourseCode',
        'courseOfferingReference.schoolId': 'SchoolId',
        'courseOfferingReference.schoolYear': 'SchoolYear',
        'sectionIdentifier': 'SectionIdentifier',
        'courseOfferingReference.sessionName': 'SessionName'
    })

    result_data_frame = result_data_frame[columns]

    return result_data_frame[
        columns
    ]


def class_period_dim(school_year) -> data_frame_generation_result:
    return class_period_dim_data_frame(
        file_name="classPeriodDim.parquet",
        columns=RESULT_COLUMNS,
        school_year=school_year
    )

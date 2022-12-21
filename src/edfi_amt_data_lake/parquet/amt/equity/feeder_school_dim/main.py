# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from datetime import date

from decouple import config

from edfi_amt_data_lake.helper.data_frame_generation_result import (
    data_frame_generation_result,
)
from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    create_parquet_file,
    jsonNormalize,
    pdMerge,
    renameColumns,
    subset,
    to_datetime_key,
)

ENDPOINT_STUDENT_FEEDER_SCHOOL_ASSOCIATION = 'feederSchoolAssociations'
ENDPOINT_SCHOOL = 'schools'
RESULT_COLUMNS = [
    'feederSchoolUniqueKey',
    'schoolKey',
    'feederSchoolKey',
    'feederSchoolName'
]


@create_parquet_file
def feeder_school_dim_data_frame(
    file_name: str,
    columns: list[str],
    school_year: int
):
    feeder_school_association_content = getEndpointJson(ENDPOINT_STUDENT_FEEDER_SCHOOL_ASSOCIATION, config('SILVER_DATA_LOCATION'), school_year)
    school_content = getEndpointJson(ENDPOINT_SCHOOL, config('SILVER_DATA_LOCATION'), school_year)

    # feeder_school_association_content
    feeder_school_association_normalized = jsonNormalize(
        feeder_school_association_content,
        recordPath=None,
        meta=[
            'feederSchoolReference.schoolId',
            'schoolReference.schoolId'
        ],
        metaPrefix=None,
        recordPrefix='schoolFoodServiceProgramServices_',
        errors='ignore'
    )

    if feeder_school_association_normalized.empty:
        return None

    if 'endDate' in feeder_school_association_normalized:
        feeder_school_association_normalized['endDate'] = to_datetime_key(feeder_school_association_normalized, 'endDate')
        feeder_school_association_normalized['date_now'] = date.today()
        feeder_school_association_normalized['date_now'] = to_datetime_key(feeder_school_association_normalized, 'date_now')
        feeder_school_association_normalized = feeder_school_association_normalized[feeder_school_association_normalized['endDate'] >= feeder_school_association_normalized['date_now']]

    # Select needed columns.
    feeder_school_association_normalized = subset(feeder_school_association_normalized, [
        'feederSchoolReference.schoolId',
        'schoolReference.schoolId'
    ])

    feeder_school_association_normalized = renameColumns(feeder_school_association_normalized, {
        'feederSchoolReference.schoolId': 'feederSchoolKey',
        'schoolReference.schoolId': 'schoolKey'
    })

    feeder_school_association_normalized['schoolKey'] = feeder_school_association_normalized['schoolKey'].astype(str)

    # schools
    school_normalized = jsonNormalize(
        school_content,
        recordPath=None,
        meta=[
            'schoolId',
            'nameOfInstitution'
        ],
        metaPrefix=None,
        recordPrefix='programTypeDescriptor_',
        errors='ignore'
    )

    # Select needed columns.
    school_normalized = subset(school_normalized, [
        'schoolId',
        'nameOfInstitution'
    ])

    # feeder_school_association -school
    result_data_frame = pdMerge(
        left=feeder_school_association_normalized,
        right=school_normalized,
        how='inner',
        leftOn=['feederSchoolKey'],
        rightOn=['schoolId'],
        suffixLeft='_feeder_school',
        suffixRight='_school'
    )

    result_data_frame = renameColumns(result_data_frame, {
        'nameOfInstitution': 'feederSchoolName'
    })

    result_data_frame['feederSchoolKey'] = result_data_frame['feederSchoolKey'].astype(str)

    result_data_frame['feederSchoolUniqueKey'] = (
        result_data_frame['schoolKey']
        + '-' + result_data_frame['feederSchoolKey']
    )

    return subset(result_data_frame, columns)


def feeder_school_dim(school_year) -> data_frame_generation_result:
    return feeder_school_dim_data_frame(
        file_name="equity_FeederSchoolDim.parquet",
        columns=RESULT_COLUMNS,
        school_year=school_year
    )

# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import pandas as pd
from decouple import config

from edfi_amt_data_lake.helper.data_frame_generation_result import (
    data_frame_generation_result,
)
from edfi_amt_data_lake.parquet.Common.descriptor_mapping import get_descriptor_constant
from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    addColumnIfNotExists,
    get_descriptor_code_value_from_uri,
    jsonNormalize,
    pdMerge,
    renameColumns,
    save_parquet_file,
    subset,
)

ENDPOINT_SCHOOLS = 'schools'
ENDPOINT_LOCALEDUCATIONAGENCIES = 'localEducationAgencies'
ENDPOINT_STATEEDUCATIONAGENCIES = 'stateEducationAgencies'
ENDPOINT_EDUCATIONSERVICECENTERS = 'educationServiceCenters'
RESULT_COLUMNS = [
    'SchoolKey',
    'SchoolName',
    'SchoolType',
    'SchoolAddress',
    'SchoolCity',
    'SchoolCounty',
    'SchoolState',
    'LocalEducationAgencyName',
    'LocalEducationAgencyKey',
    'StateEducationAgencyName',
    'StateEducationAgencyKey',
    'EducationServiceCenterName',
    'EducationServiceCenterKey'
]


def school_dim_data_frame(school_year) -> pd.DataFrame:
    schoolsContent = getEndpointJson(ENDPOINT_SCHOOLS, config('SILVER_DATA_LOCATION'), school_year)
    localEducationAgenciesContent = getEndpointJson(ENDPOINT_LOCALEDUCATIONAGENCIES, config('SILVER_DATA_LOCATION'), school_year)
    stateEducationAgenciesContent = getEndpointJson(ENDPOINT_STATEEDUCATIONAGENCIES, config('SILVER_DATA_LOCATION'), school_year)
    educationServiceCentersContent = getEndpointJson(ENDPOINT_EDUCATIONSERVICECENTERS, config('SILVER_DATA_LOCATION'), school_year)
    schoolsContentNormalized = jsonNormalize(
        data=schoolsContent,
        recordPath=['addresses'],
        meta=[
            'schoolId',
            'nameOfInstitution',
            'schoolTypeDescriptor',
            ['localEducationAgencyReference', 'localEducationAgencyId'],
        ],
        recordMeta=[
            'addressTypeDescriptor',
            'stateAbbreviationDescriptor',
            'streetNumberName',
            'city',
            'nameOfCounty'
        ],
        metaPrefix=None,
        recordPrefix='address_',
        errors='ignore'
    )
    # Local Education Agency Join
    localEducationAgenciesContentNormalized = jsonNormalize(
        localEducationAgenciesContent,
        recordPath=None,
        meta=[
            'localEducationAgencyId',
            'nameOfInstitution',
            'educationServiceCenterReference.educationServiceCenterId',
            'stateEducationAgencyReference.stateEducationAgencyId'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    result_data_frame = pdMerge(
        left=schoolsContentNormalized,
        right=localEducationAgenciesContentNormalized,
        how='left',
        leftOn=['localEducationAgencyReference.localEducationAgencyId'],
        rightOn=['localEducationAgencyId'],
        suffixLeft='_schools',
        suffixRight='_localEducationAgencies'
    )
    if result_data_frame.empty:
        return None
    # Education Service Center Join
    educationServiceCentersContentNormalized = jsonNormalize(
        educationServiceCentersContent,
        recordPath=None,
        meta=[
            'educationServiceCenterId',
            'nameOfInstitution'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    if not educationServiceCentersContentNormalized.empty:
        educationServiceCentersContentNormalized = subset(educationServiceCentersContentNormalized, ['educationServiceCenterId', 'nameOfInstitution'])

        result_data_frame = pdMerge(
            left=result_data_frame,
            right=educationServiceCentersContentNormalized,
            how='left',
            leftOn=['educationServiceCenterReference.educationServiceCenterId'],
            rightOn=['educationServiceCenterId'],
            suffixLeft=None,
            suffixRight='_educationServiceCenters'
        )

    # State Education Agency Join
    if stateEducationAgenciesContent != '':
        stateEducationAgenciesContentNormalized = jsonNormalize(
            stateEducationAgenciesContent,
            recordPath=None,
            meta=[
                'stateEducationAgencyId',
                'nameOfInstitution'
            ],
            metaPrefix=None,
            recordPrefix=None,
            errors='ignore'
        )

        if not stateEducationAgenciesContentNormalized.empty:
            stateEducationAgenciesContentNormalized = subset(stateEducationAgenciesContentNormalized, ['stateEducationAgencyId', 'nameOfInstitution'])

            result_data_frame = pdMerge(
                left=result_data_frame,
                right=stateEducationAgenciesContentNormalized,
                how='left',
                leftOn=['stateEducationAgencyReference.stateEducationAgencyId'],
                rightOn=['stateEducationAgencyId'],
                suffixLeft=None,
                suffixRight='_stateEducationAgencies'
            )
    else:
        addColumnIfNotExists(result_data_frame, 'stateEducationAgencyId')
        addColumnIfNotExists(result_data_frame, 'nameOfInstitution_stateEducationAgencies')
    result_data_frame = get_descriptor_constant(result_data_frame, 'address_addressTypeDescriptor')
    result_data_frame = result_data_frame[result_data_frame["address_addressTypeDescriptor_constantName"].str.contains('Address.Physical', na=False)]
    # Removes namespace from Address Type Descriptor
    get_descriptor_code_value_from_uri(result_data_frame, 'address_addressTypeDescriptor')
    # Removes namespace from School Type Descriptor
    get_descriptor_code_value_from_uri(result_data_frame, 'schoolTypeDescriptor')
    # Removes namespace from State Abbreviation Descriptor
    get_descriptor_code_value_from_uri(result_data_frame, 'address_stateAbbreviationDescriptor')
    # Creates concatanation for Address field
    result_data_frame['SchoolAddress'] = (
        result_data_frame['address_streetNumberName']
        + ', ' + result_data_frame['address_city']
        + ' ' + result_data_frame['address_stateAbbreviationDescriptor']
        + ' ' + result_data_frame['address_nameOfCounty']
    )
    # Rename columns to match AMT
    result_data_frame = renameColumns(result_data_frame, {
        'schoolId': 'SchoolKey',
        'nameOfInstitution_schools': 'SchoolName',
        'schoolTypeDescriptor': 'SchoolType',
        'address_city': 'SchoolCity',
        'address_nameOfCounty': 'SchoolCounty',
        'address_stateAbbreviationDescriptor': 'SchoolState',
        'nameOfInstitution_localEducationAgencies': 'LocalEducationAgencyName',
        'localEducationAgencyId': 'LocalEducationAgencyKey',
        'nameOfInstitution_stateEducationAgencies': 'StateEducationAgencyName',
        'stateEducationAgencyId': 'StateEducationAgencyKey',
        'nameOfInstitution': 'EducationServiceCenterName',
        'educationServiceCenterId': 'EducationServiceCenterKey'
    })
    # Reorder columns to match AMT
    return result_data_frame[
        RESULT_COLUMNS
    ]


def school_dim(school_year) -> data_frame_generation_result:
    try:
        result = data_frame_generation_result(
            data_frame=school_dim_data_frame(school_year),
            columns=RESULT_COLUMNS
        )
        save_parquet_file(result, f"{config('PARQUET_FILES_LOCATION')}", "schoolDim.parquet", school_year)
        return result
    except Exception as data_frame_exception:
        return data_frame_generation_result(
            successful=False,
            exception=data_frame_exception
        )

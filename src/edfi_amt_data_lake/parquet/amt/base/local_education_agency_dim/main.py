# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import pandas as pd
from decouple import config

from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    get_descriptor_code_value_from_uri,
    get_reference_from_href,
    jsonNormalize,
    pdMerge,
    renameColumns,
    replace_null,
    saveParquetFile,
    subset,
)

ENDPOINT_LOCAL_EDUCATION_AGENCY = 'localEducationAgencies'
ENDPOINT_STATE_EDUCATION_AGENCY = 'stateEducationAgencies'
ENDPOINT_EDUCATION_SERVICE_CENTER = 'educationServiceCenters'


def local_education_agency_dataframe(school_year) -> pd.DataFrame:
    local_education_agency_content = getEndpointJson(ENDPOINT_LOCAL_EDUCATION_AGENCY, config('SILVER_DATA_LOCATION'), school_year)
    state_education_agency_content = getEndpointJson(ENDPOINT_STATE_EDUCATION_AGENCY, config('SILVER_DATA_LOCATION'), school_year)
    education_service_center_content = getEndpointJson(ENDPOINT_EDUCATION_SERVICE_CENTER, config('SILVER_DATA_LOCATION'), school_year)
    ############################
    # localEducationAgencies
    ############################
    local_education_agency_normalize = jsonNormalize(
        local_education_agency_content,
        recordPath=None,
        meta=[
            'localEducationAgencyId',
            'nameOfInstitution',
            'educationServiceCenterReference.link.href',
            'localEducationAgencyCategoryDescriptor',
            'parentLocalEducationAgencyReference.localEducationAgencyId',
            'stateEducationAgencyReference.link.href',
            'charterStatusDescriptor'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    replace_null(local_education_agency_normalize, 'charterStatusDescriptor', '')
    replace_null(local_education_agency_normalize, 'parentLocalEducationAgencyReference.localEducationAgencyId', '')
    get_descriptor_code_value_from_uri(
        local_education_agency_normalize,
        'localEducationAgencyCategoryDescriptor'
    )
    get_descriptor_code_value_from_uri(
        local_education_agency_normalize,
        'charterStatusDescriptor'
    )
    get_reference_from_href(
        local_education_agency_normalize,
        'educationServiceCenterReference.link.href',
        'educationServiceCenterReferenceId'
    )
    get_reference_from_href(
        local_education_agency_normalize,
        'stateEducationAgencyReference.link.href',
        'stateEducationAgencyReferenceId'
    )
    local_education_agency_normalize = renameColumns(local_education_agency_normalize, {
        'localEducationAgencyId': 'localEducationAgencyKey',
        'nameOfInstitution': 'localEducationAgencyName',
        'localEducationAgencyCategoryDescriptor': 'localEducationAgencyType',
        'charterStatusDescriptor': 'localEducationAgencyCharterStatus',
        'parentLocalEducationAgencyReference.localEducationAgencyId': 'localEducationAgencyParentLocalEducationAgencyKey'
    })
    # Select needed columns.
    local_education_agency_normalize = subset(local_education_agency_normalize, [
        'localEducationAgencyKey',
        'localEducationAgencyName',
        'localEducationAgencyType',
        'localEducationAgencyCharterStatus',
        'localEducationAgencyParentLocalEducationAgencyKey',
        'educationServiceCenterReferenceId',
        'stateEducationAgencyReferenceId',
    ])
    ############################
    # stateEducationAgencies
    ############################
    state_education_agency_normalize = jsonNormalize(
        state_education_agency_content,
        recordPath=None,
        meta=[
            'id',
            'nameOfInstitution',
            'stateEducationAgencyId',
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    state_education_agency_normalize = renameColumns(state_education_agency_normalize, {
        'id': 'stateEducationAgencyReferenceId',
        'nameOfInstitution': 'localEducationAgencyStateEducationAgencyName',
        'stateEducationAgencyId': 'localEducationAgencyStateEducationAgencyKey',
    })
    # Select needed columns.
    state_education_agency_normalize = subset(state_education_agency_normalize, [
        'stateEducationAgencyReferenceId',
        'localEducationAgencyStateEducationAgencyName',
        'localEducationAgencyStateEducationAgencyKey',
    ])
    ############################
    # educationServiceCenters
    ############################
    education_service_center_normalize = jsonNormalize(
        education_service_center_content,
        recordPath=None,
        meta=[
            'id',
            'nameOfInstitution',
            'educationServiceCenterId',
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    education_service_center_normalize = renameColumns(education_service_center_normalize, {
        'id': 'educationServiceCenterReferenceId',
        'nameOfInstitution': 'localEducationAgencyServiceCenterName',
        'educationServiceCenterId': 'localEducationAgencyServiceCenterKey',
    })
    # Select needed columns.
    education_service_center_normalize = subset(education_service_center_normalize, [
        'educationServiceCenterReferenceId',
        'localEducationAgencyServiceCenterName',
        'localEducationAgencyServiceCenterKey',
    ])
    ############################
    # join dataframe
    ############################
    result_data_frame = pdMerge(
        left=local_education_agency_normalize,
        right=state_education_agency_normalize,
        how='left',
        leftOn=['stateEducationAgencyReferenceId'],
        rightOn=['stateEducationAgencyReferenceId'],
        suffixLeft='_localEducationAgencies',
        suffixRight='_stateEducationAgencies'
    )
    result_data_frame = pdMerge(
        left=result_data_frame,
        right=education_service_center_normalize,
        how='left',
        leftOn=['educationServiceCenterReferenceId'],
        rightOn=['educationServiceCenterReferenceId'],
        suffixLeft='_localEducationAgencies',
        suffixRight='_educationServiceCenter'
    )
    # Select needed columns.
    result_data_frame = subset(result_data_frame, [
        'localEducationAgencyKey',
        'localEducationAgencyName',
        'localEducationAgencyType',
        'localEducationAgencyParentLocalEducationAgencyKey',
        'localEducationAgencyStateEducationAgencyName',
        'localEducationAgencyStateEducationAgencyKey',
        'localEducationAgencyServiceCenterName',
        'localEducationAgencyServiceCenterKey',
        'localEducationAgencyCharterStatus',
    ])
    return result_data_frame


def local_education_agency_dim(school_year) -> None:
    result_data_frame = local_education_agency_dataframe(school_year)
    saveParquetFile(result_data_frame, f"{config('PARQUET_FILES_LOCATION')}", "localEducationAgencyDim.parquet", school_year)

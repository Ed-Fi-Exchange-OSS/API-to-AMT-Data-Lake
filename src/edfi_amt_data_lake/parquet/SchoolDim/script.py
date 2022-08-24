# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from distutils.util import subst_vars
from operator import contains
from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import jsonNormalize, pdMerge, toCsv, subset, renameColumns


ENDPOINT_SCHOOLS = 'schools'
ENDPOINT_LOCALEDUCATIONAGENCIES = 'localEducationAgencies'
ENDPOINT_STATEEDUCATIONAGENCIES = 'stateEducationAgencies'

def schoolDim() -> None:
    schoolsContent = getEndpointJson(ENDPOINT_SCHOOLS)
    localEducationAgenciesContent = getEndpointJson(ENDPOINT_LOCALEDUCATIONAGENCIES)
    stateEducationAgenciesContent = getEndpointJson(ENDPOINT_STATEEDUCATIONAGENCIES)

    schoolsContentNormalized =  jsonNormalize(
        schoolsContent,
        ['addresses'],
        ['schoolId', 'nameOfInstitution', 'schoolTypeDescriptor', 'localEducationAgencyReference.localEducationAgencyId' ],
        None,
        'address',
        'ignore'
    )

    localEducationAgenciesContentNormalized = jsonNormalize(
        localEducationAgenciesContent,
        recordPath=None,
        meta=None,
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    localEducationAgenciesContentNormalized = subset(localEducationAgenciesContentNormalized, ['localEducationAgencyId', 'nameOfInstitution'])

    restultDataFrame = pdMerge(
        left=schoolsContentNormalized, 
        right=localEducationAgenciesContentNormalized,
        how='left',
        leftOn=['localEducationAgencyReference.localEducationAgencyId'],
        rigthOn=['localEducationAgencyId'],
        suffixLeft='_schools',
        suffixRight='_localEducationAgencies'
    )

    stateEducationAgenciesContentNormalized = jsonNormalize(
        stateEducationAgenciesContent,
        recordPath=None,
        meta=None,
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    if stateEducationAgenciesContentNormalized.columns > 0:
        stateEducationAgenciesContentNormalized = subset(stateEducationAgenciesContentNormalized, ['stateEducationAgencyId', 'nameOfInstitution'])

        restultDataFrame = pdMerge(
            left=restultDataFrame, 
            right=stateEducationAgenciesContentNormalized,
            how='left',
            leftOn=['stateEducationAgencyReference.stateEducationAgencyId'],
            rigthOn=['stateEducationAgencyId'],
            suffixLeft=None,
            suffixRight='_stateEducationAgencies'
        )

    restultDataFrame = renameColumns(restultDataFrame, 
        {
            'schoolId': 'SchoolKey',
            'nameOfInstitution_schools': 'SchoolName',
            'schoolTypeDescriptor': 'SchoolType',
            'addresscity': 'SchoolCity',
            'addressnameOfCounty': 'SchoolCounty',
            'addressstateAbbreviationDescriptor': 'SchoolState',
            'nameOfInstitution_localEducationAgencies': 'LocalEducationAgencyName',
            'localEducationAgencyReference.localEducationAgencyId': 'LocalEducationAgencyKey',
            '': 'StateEducationAgencyName',
            '': 'StateEducationAgencyKey',
            '': 'EducationServiceCenterName',
            '': 'EducationServiceCenterKey'
        })

    restultDataFrame = restultDataFrame[restultDataFrame["addressaddressTypeDescriptor"].str.contains('Physical')]

    toCsv(restultDataFrame, "C:\\temp\\edfi\\restultDataFrame.csv")

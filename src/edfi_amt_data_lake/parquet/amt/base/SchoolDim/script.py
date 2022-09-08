# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from distutils.util import subst_vars
from operator import contains
from decouple import config
from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import jsonNormalize, pdMerge, subset, renameColumns, saveParquetFile, addColumnIfNotExists

ENDPOINT_SCHOOLS = 'schools'
ENDPOINT_LOCALEDUCATIONAGENCIES = 'localEducationAgencies'
ENDPOINT_STATEEDUCATIONAGENCIES = 'stateEducationAgencies'
ENDPOINT_EDUCATIONSERVICECENTERS = 'educationServiceCenters'

def schoolDim(school_year="") -> None:
    schoolsContent = getEndpointJson(ENDPOINT_SCHOOLS, config('SILVER_DATA_LOCATION'),school_year)
    localEducationAgenciesContent = getEndpointJson(ENDPOINT_LOCALEDUCATIONAGENCIES, config('SILVER_DATA_LOCATION'))
    stateEducationAgenciesContent = getEndpointJson(ENDPOINT_STATEEDUCATIONAGENCIES, config('SILVER_DATA_LOCATION'))
    educationServiceCentersContent = getEndpointJson(ENDPOINT_EDUCATIONSERVICECENTERS, config('SILVER_DATA_LOCATION'))

    schoolsContentNormalized =  jsonNormalize(
        schoolsContent,
        ['addresses'],
        ['schoolId', 'nameOfInstitution', 'schoolTypeDescriptor', ['localEducationAgencyReference','localEducationAgencyId' ]],
        None,
        'address',
        'ignore'
    )

    # Local Education Agency Join
    localEducationAgenciesContentNormalized = jsonNormalize(
        localEducationAgenciesContent,
        recordPath=None,
        meta=None,
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    
    addColumnIfNotExists(localEducationAgenciesContentNormalized, 'stateEducationAgencyReference.stateEducationAgencyId')

    localEducationAgenciesContentNormalized = subset(localEducationAgenciesContentNormalized, ['localEducationAgencyId', 'nameOfInstitution', 'educationServiceCenterReference.educationServiceCenterId', 'stateEducationAgencyReference.stateEducationAgencyId'])

    restultDataFrame = pdMerge(
        left=schoolsContentNormalized, 
        right=localEducationAgenciesContentNormalized,
        how='left',
        leftOn=['localEducationAgencyReference.localEducationAgencyId'],
        rigthOn=['localEducationAgencyId'],
        suffixLeft='_schools',
        suffixRight='_localEducationAgencies'
    )

    # Education Service Center Join
    educationServiceCentersContentNormalized = jsonNormalize(
        educationServiceCentersContent,
        recordPath=None,
        meta=None,
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    if not educationServiceCentersContentNormalized.empty:
        educationServiceCentersContentNormalized = subset(educationServiceCentersContentNormalized, ['educationServiceCenterId', 'nameOfInstitution'])

        restultDataFrame = pdMerge(
            left=restultDataFrame, 
            right=educationServiceCentersContentNormalized,
            how='left',
            leftOn=['educationServiceCenterReference.educationServiceCenterId'],
            rigthOn=['educationServiceCenterId'],
            suffixLeft=None,
            suffixRight='_educationServiceCenters'
        )

    # State Education Agency Join
    if stateEducationAgenciesContent != '':
        stateEducationAgenciesContentNormalized = jsonNormalize(
            stateEducationAgenciesContent,
            recordPath=None,
            meta=None,
            metaPrefix=None,
            recordPrefix=None,
            errors='ignore'
        )

        if not stateEducationAgenciesContentNormalized.empty:
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
    else:
        addColumnIfNotExists(restultDataFrame, 'stateEducationAgencyId')
        addColumnIfNotExists(restultDataFrame, 'nameOfInstitution_stateEducationAgencies')

    restultDataFrame = restultDataFrame[restultDataFrame["addressaddressTypeDescriptor"].str.contains('Physical')]

    # Removes namespace from Address Type Descriptor
    if not restultDataFrame['addressaddressTypeDescriptor'].empty:
        if len(restultDataFrame['addressaddressTypeDescriptor'].str.split('#')) > 0:
            restultDataFrame["addressaddressTypeDescriptor"] = restultDataFrame["addressaddressTypeDescriptor"].str.split("#").str.get(1)

    # Removes namespace from School Type Descriptor
    if not restultDataFrame['schoolTypeDescriptor'].empty:
        if len(restultDataFrame['schoolTypeDescriptor'].str.split('#')) > 0:
            restultDataFrame["schoolTypeDescriptor"] = restultDataFrame["schoolTypeDescriptor"].str.split("#").str.get(1)

    # Removes namespace from State Abbreviation Descriptor
    if not restultDataFrame['addressstateAbbreviationDescriptor'].empty:
        if len(restultDataFrame['addressstateAbbreviationDescriptor'].str.split('#')) > 0:
            restultDataFrame["addressstateAbbreviationDescriptor"] = restultDataFrame["addressstateAbbreviationDescriptor"].str.split("#").str.get(1)

    # Creates concatanation for Address field
    restultDataFrame['SchoolAddress'] = (
            restultDataFrame['addressstreetNumberName'] 
            + ', ' + restultDataFrame['addresscity'] 
            + ' ' + restultDataFrame['addressstateAbbreviationDescriptor'] 
            + ' ' + restultDataFrame['addressnameOfCounty']
        )

    # Rename columns to match AMT
    restultDataFrame = renameColumns(restultDataFrame, 
        {
            'schoolId': 'SchoolKey',
            'nameOfInstitution_schools': 'SchoolName',
            'schoolTypeDescriptor': 'SchoolType',
            'addresscity': 'SchoolCity',
            'addressnameOfCounty': 'SchoolCounty',
            'addressstateAbbreviationDescriptor': 'SchoolState',
            'nameOfInstitution_localEducationAgencies': 'LocalEducationAgencyName',
            'localEducationAgencyId': 'LocalEducationAgencyKey',
            'nameOfInstitution_stateEducationAgencies': 'StateEducationAgencyName',
            'stateEducationAgencyId': 'StateEducationAgencyKey',
            'nameOfInstitution': 'EducationServiceCenterName',
            'educationServiceCenterId': 'EducationServiceCenterKey'
        })

    # Reorder columns to match AMT
    restultDataFrame = restultDataFrame[[
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
        ]]

    saveParquetFile(restultDataFrame, f"{config('PARQUET_FILES_LOCATION')}SchoolDim.parquet")
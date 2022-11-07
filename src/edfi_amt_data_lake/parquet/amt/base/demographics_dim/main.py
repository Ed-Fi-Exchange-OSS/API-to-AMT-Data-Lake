# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import pandas as pd
from decouple import config

from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    jsonNormalize,
    pd_concat,
    pdMerge,
    saveParquetFile,
    subset,
)

ENDPOINT_SCHOOL_YEAR_TYPE = 'schoolYearTypes'
ENDPOINT_COHORT_YEAR_TYPE_DESCRIPTOR = 'cohortYearTypeDescriptors'
ENDPOINT_DISABILITY_DESCRIPTOR = 'disabilityDescriptors'
ENDPOINT_DISABILITY_DESIGNATION_DESCRIPTOR = 'disabilityDesignationDescriptors'
ENDPOINT_LANGUAGE_DESCRIPTOR = 'languageDescriptors'
ENDPOINT_LANGUAGE_USE_DESCRIPTOR = 'languageUseDescriptors'
ENDPOINT_RACE_DESCRIPTOR = 'raceDescriptors'
ENDPOINT_TRIBAL_AFFILIATION_DESCRIPTOR = 'tribalAffiliationDescriptors'
ENDPOINT_STUDENT_CHARACTERISTIC_DESCRIPTOR = 'studentCharacteristicDescriptors'


def demographics_dim_dataframe(school_year) -> pd.DataFrame:
    school_year_type_content = getEndpointJson(ENDPOINT_SCHOOL_YEAR_TYPE, config('SILVER_DATA_LOCATION'), school_year)
    cohort_year_type_descriptor_content = getEndpointJson(ENDPOINT_COHORT_YEAR_TYPE_DESCRIPTOR, config('SILVER_DATA_LOCATION'), school_year)
    disability_descriptor_content = getEndpointJson(ENDPOINT_DISABILITY_DESCRIPTOR, config('SILVER_DATA_LOCATION'), school_year)
    disability_designation_descriptor_content = getEndpointJson(ENDPOINT_DISABILITY_DESIGNATION_DESCRIPTOR, config('SILVER_DATA_LOCATION'), school_year)
    language_descriptor_content = getEndpointJson(ENDPOINT_LANGUAGE_DESCRIPTOR, config('SILVER_DATA_LOCATION'), school_year)
    language_use_descriptor_content = getEndpointJson(ENDPOINT_LANGUAGE_USE_DESCRIPTOR, config('SILVER_DATA_LOCATION'), school_year)
    race_descriptor_content = getEndpointJson(ENDPOINT_RACE_DESCRIPTOR, config('SILVER_DATA_LOCATION'), school_year)
    tribal_affiliation_descriptor_content = getEndpointJson(ENDPOINT_TRIBAL_AFFILIATION_DESCRIPTOR, config('SILVER_DATA_LOCATION'), school_year)
    student_characteristic_descriptor_content = getEndpointJson(ENDPOINT_STUDENT_CHARACTERISTIC_DESCRIPTOR, config('SILVER_DATA_LOCATION'), school_year)
    ############################
    # schoolYearTypes
    ############################
    school_year_type_normalize = jsonNormalize(
        school_year_type_content,
        recordPath=None,
        meta=[
            'schoolYear',
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    school_year_type_normalize['fakeKey'] = -1
    school_year_type_normalize['schoolYear'] = school_year_type_normalize['schoolYear'].astype(str)
    # Select needed columns.
    school_year_type_normalize = subset(school_year_type_normalize, [
        'fakeKey',
        'schoolYear'
    ])
    ############################
    # cohortYearTypeDescriptors
    ############################
    cohort_year_type_descriptor_normalize = jsonNormalize(
        cohort_year_type_descriptor_content,
        recordPath=None,
        meta=[
            'codeValue',
            'descriptor',
            'shortDescription'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    cohort_year_type_descriptor_normalize['fakeKey'] = -1
    # Select needed columns.
    cohort_year_type_descriptor_normalize = subset(cohort_year_type_descriptor_normalize, [
        'fakeKey',
        'codeValue',
        'shortDescription'
    ])
    ############################
    # Merge cohortYear
    ############################
    cohort_year_type_descriptor_normalize = pdMerge(
        left=cohort_year_type_descriptor_normalize,
        right=school_year_type_normalize,
        how='outer',
        leftOn=['fakeKey'],
        rightOn=['fakeKey'],
        suffixLeft=None,
        suffixRight=None
    )
    cohort_year_type_descriptor_normalize['demographicParentKey'] = 'CohortYear'
    cohort_year_type_descriptor_normalize['demographicLabel'] = (
        cohort_year_type_descriptor_normalize['schoolYear']
        + '-' + cohort_year_type_descriptor_normalize['codeValue']
    )
    cohort_year_type_descriptor_normalize['demographicKey'] = (
        cohort_year_type_descriptor_normalize['demographicParentKey']
        + ':' + cohort_year_type_descriptor_normalize['demographicLabel']
    )
    cohort_year_type_descriptor_normalize['shortDescription'] = cohort_year_type_descriptor_normalize['shortDescription']
    # Select needed columns.
    cohort_year_type_descriptor_normalize = subset(cohort_year_type_descriptor_normalize, [
        'demographicKey',
        'demographicParentKey',
        'demographicLabel',
        'shortDescription'
    ])
    ############################
    # disabilityDescriptors
    ############################
    disability_descriptor_normalize = jsonNormalize(
        disability_descriptor_content,
        recordPath=None,
        meta=[
            'codeValue',
            'descriptor',
            'shortDescription'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    disability_descriptor_normalize['demographicParentKey'] = (
        'Disability'
    )
    disability_descriptor_normalize['demographicLabel'] = (
        disability_descriptor_normalize['codeValue']
    )
    disability_descriptor_normalize['demographicKey'] = (
        disability_descriptor_normalize['demographicParentKey']
        + ':' + disability_descriptor_normalize['demographicLabel']
    )
    disability_descriptor_normalize['shortDescription'] = disability_descriptor_normalize['shortDescription']
    # Select needed columns.
    disability_descriptor_normalize = subset(disability_descriptor_normalize, [
        'demographicKey',
        'demographicParentKey',
        'demographicLabel',
        'shortDescription'
    ])
    ############################
    # disabilityDesignationDescriptors
    ############################
    disability_designation_descriptor_normalize = jsonNormalize(
        disability_designation_descriptor_content,
        recordPath=None,
        meta=[
            'codeValue',
            'shortDescription'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    disability_designation_descriptor_normalize['demographicParentKey'] = (
        'DisabilityDesignation'
    )
    disability_designation_descriptor_normalize['demographicLabel'] = (
        disability_designation_descriptor_normalize['codeValue']
    )
    disability_designation_descriptor_normalize['demographicKey'] = (
        disability_designation_descriptor_normalize['demographicParentKey']
        + ':' + disability_designation_descriptor_normalize['demographicLabel']
    )
    disability_designation_descriptor_normalize['shortDescription'] = disability_designation_descriptor_normalize['shortDescription']
    # Select needed columns.
    disability_designation_descriptor_normalize = subset(disability_designation_descriptor_normalize, [
        'demographicKey',
        'demographicParentKey',
        'demographicLabel',
        'shortDescription'
    ])
    ############################
    # languageDescriptors
    ############################
    language_descriptor_normalize = jsonNormalize(
        language_descriptor_content,
        recordPath=None,
        meta=[
            'codeValue',
            'descriptor',
            'shortDescription'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    language_descriptor_normalize['demographicParentKey'] = (
        'Language'
    )
    language_descriptor_normalize['demographicLabel'] = (
        language_descriptor_normalize['codeValue']
    )
    language_descriptor_normalize['demographicKey'] = (
        language_descriptor_normalize['demographicParentKey']
        + ':' + language_descriptor_normalize['demographicLabel']
    )
    language_descriptor_normalize['shortDescription'] = language_descriptor_normalize['shortDescription']
    # Select needed columns.
    language_descriptor_normalize = subset(language_descriptor_normalize, [
        'demographicKey',
        'demographicParentKey',
        'demographicLabel',
        'shortDescription'
    ])
    ############################
    # languageUseDescriptors
    ############################
    language_use_descriptor_normalize = jsonNormalize(
        language_use_descriptor_content,
        recordPath=None,
        meta=[
            'codeValue',
            'descriptor',
            'shortDescription'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    language_use_descriptor_normalize['demographicParentKey'] = (
        'LanguageUse'
    )
    language_use_descriptor_normalize['demographicLabel'] = (
        language_use_descriptor_normalize['codeValue']
    )
    language_use_descriptor_normalize['demographicKey'] = (
        language_use_descriptor_normalize['demographicParentKey']
        + ':' + language_use_descriptor_normalize['demographicLabel']
    )
    language_use_descriptor_normalize['shortDescription'] = language_use_descriptor_normalize['shortDescription']
    # Select needed columns.
    language_use_descriptor_normalize = subset(language_use_descriptor_normalize, [
        'demographicKey',
        'demographicParentKey',
        'demographicLabel',
        'shortDescription'
    ])
    ############################
    # raceDescriptors
    ############################
    race_descriptor_normalize = jsonNormalize(
        race_descriptor_content,
        recordPath=None,
        meta=[
            'codeValue',
            'descriptor',
            'shortDescription'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    race_descriptor_normalize['demographicParentKey'] = (
        'Race'
    )
    race_descriptor_normalize['demographicLabel'] = (
        race_descriptor_normalize['codeValue']
    )
    race_descriptor_normalize['demographicKey'] = (
        race_descriptor_normalize['demographicParentKey']
        + ':' + race_descriptor_normalize['demographicLabel']
    )
    race_descriptor_normalize['shortDescription'] = race_descriptor_normalize['shortDescription']
    # Select needed columns.
    race_descriptor_normalize = subset(race_descriptor_normalize, [
        'demographicKey',
        'demographicParentKey',
        'demographicLabel',
        'shortDescription'
    ])
    ############################
    # tribalAffiliationDescriptor
    ############################
    tribal_affiliation_descriptor_normalize = jsonNormalize(
        tribal_affiliation_descriptor_content,
        recordPath=None,
        meta=[
            'codeValue',
            'descriptor',
            'shortDescription'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    tribal_affiliation_descriptor_normalize['demographicParentKey'] = (
        'TribalAffiliation'
    )
    tribal_affiliation_descriptor_normalize['demographicLabel'] = (
        tribal_affiliation_descriptor_normalize['codeValue']
    )
    tribal_affiliation_descriptor_normalize['demographicKey'] = (
        tribal_affiliation_descriptor_normalize['demographicParentKey']
        + ':' + tribal_affiliation_descriptor_normalize['demographicLabel']
    )
    tribal_affiliation_descriptor_normalize['shortDescription'] = tribal_affiliation_descriptor_normalize['shortDescription']
    # Select needed columns.
    tribal_affiliation_descriptor_normalize = subset(tribal_affiliation_descriptor_normalize, [
        'demographicKey',
        'demographicParentKey',
        'demographicLabel',
        'shortDescription'
    ])
    ############################
    # studentCharacteristicDescriptor
    ############################
    student_characteristic_descriptor_normalize = jsonNormalize(
        student_characteristic_descriptor_content,
        recordPath=None,
        meta=[
            'codeValue',
            'descriptor',
            'shortDescription'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    student_characteristic_descriptor_normalize['demographicParentKey'] = (
        'StudentCharacteristic'
    )
    student_characteristic_descriptor_normalize['demographicLabel'] = (
        student_characteristic_descriptor_normalize['codeValue']
    )
    student_characteristic_descriptor_normalize['demographicKey'] = (
        student_characteristic_descriptor_normalize['demographicParentKey']
        + ':' + student_characteristic_descriptor_normalize['demographicLabel']
    )
    student_characteristic_descriptor_normalize['shortDescription'] = student_characteristic_descriptor_normalize['shortDescription']
    # Select needed columns.
    student_characteristic_descriptor_normalize = subset(student_characteristic_descriptor_normalize, [
        'demographicKey',
        'demographicParentKey',
        'demographicLabel',
        'shortDescription'
    ])
    ############################
    # CONCAT RESULTS
    ############################
    result_data_frame = pd_concat(
        [
            cohort_year_type_descriptor_normalize,
            disability_descriptor_normalize,
            disability_designation_descriptor_normalize,
            language_descriptor_normalize,
            language_use_descriptor_normalize,
            race_descriptor_normalize,
            tribal_affiliation_descriptor_normalize,
            student_characteristic_descriptor_normalize
        ],
    )
    return result_data_frame


def demographics_dim(school_year) -> None:
    result_data_frame = demographics_dim_dataframe(school_year)
    saveParquetFile(result_data_frame, f"{config('PARQUET_FILES_LOCATION')}", "demographicDim.parquet", school_year)

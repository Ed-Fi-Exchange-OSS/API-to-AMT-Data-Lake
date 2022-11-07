# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from datetime import date

import pandas as pd
from decouple import config

from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    add_dataframe_column,
    addColumnIfNotExists,
    get_descriptor_code_value_from_uri,
    get_reference_from_href,
    jsonNormalize,
    pd_concat,
    pdMerge,
    renameColumns,
    saveParquetFile,
    subset,
    to_datetime_key,
)

EDUCATION_ORGANIZATION_FILTER = 'LocalEducationAgency'
ENDPOINT_STUDENT_EDUCATION_ORGANIZATION_ASSOCIATION = 'studentEducationOrganizationAssociations'
ENDPOINT_SCHOOL = 'schools'
ENDPOINT_STUDENT_SCHOOL_ASSOCIATION = 'studentSchoolAssociations'


def student_local_education_agency_demographics_bridge_dataframe(school_year) -> pd.DataFrame:
    school_content = getEndpointJson(ENDPOINT_SCHOOL, config('SILVER_DATA_LOCATION'), school_year)
    student_school_association_content = getEndpointJson(ENDPOINT_STUDENT_SCHOOL_ASSOCIATION, config('SILVER_DATA_LOCATION'), school_year)
    student_education_organization_association_content = getEndpointJson(ENDPOINT_STUDENT_EDUCATION_ORGANIZATION_ASSOCIATION, config('SILVER_DATA_LOCATION'), school_year)
    demographics_dictionary_list = [
        {
            'prefix': 'Language',
            'path': 'languages',
            'descriptor': 'languageDescriptor',
            'derived_prefix': 'LanguageUse',
            'derived_path': 'uses',
            'derived_descriptor': 'languageUseDescriptor'
        },
        {
            'prefix': 'Disability',
            'path': 'disabilities',
            'descriptor': 'disabilityDescriptor',
            'derived_prefix': 'DisabilityDesignation',
            'derived_path': 'designations',
            'derived_descriptor': 'disabilityDesignationDescriptor'
        },
        {
            'prefix': 'Race',
            'path': 'races',
            'descriptor': 'raceDescriptor',
        },
        {
            'prefix': 'TribalAffiliation',
            'path': 'tribalAffiliations',
            'descriptor': 'tribalAffiliationDescriptor',
        },
        {
            'prefix': 'StudentCharacteristic',
            'path': 'studentCharacteristics',
            'descriptor': 'studentCharacteristicDescriptor',
        },
    ]
    ############################
    # student_cohort_year_normalize
    ############################
    student_cohort_year_normalize_descriptor = jsonNormalize(
        student_education_organization_association_content,
        recordPath=['cohortYears'],
        meta=[
            'id',
        ],
        metaPrefix=None,
        recordPrefix='descriptor_',
        errors='ignore'
    )
    add_dataframe_column(
        student_cohort_year_normalize_descriptor,
        [
            'descriptor_schoolYearTypeReference.schoolYear',
            'descriptor_cohortYearTypeDescriptor'
        ]
    )
    # Get Descriptor
    get_descriptor_code_value_from_uri(
        student_cohort_year_normalize_descriptor,
        'descriptor_cohortYearTypeDescriptor'
    )
    student_cohort_year_normalize_descriptor = renameColumns(
        student_cohort_year_normalize_descriptor,
        {
            'descriptor_cohortYearTypeDescriptor': 'descriptorCodeValue',
            'descriptor_schoolYearTypeReference.schoolYear': 'schoolYear'
        }
    )
    student_cohort_year_normalize_descriptor['schoolYear'] = (
        student_cohort_year_normalize_descriptor['schoolYear'].astype(str)
    )
    student_cohort_year_normalize = jsonNormalize(
        student_education_organization_association_content,
        recordPath=None,
        meta=[
            'id',
            'educationOrganizationReference.educationOrganizationId',
            'educationOrganizationReference.link.rel',
            'studentReference.studentUniqueId',
            'studentReference.link.href',
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    get_reference_from_href(
        student_cohort_year_normalize,
        'studentReference.link.href',
        'studentReferenceId',
    )
    get_reference_from_href(
        student_cohort_year_normalize,
        'educationOrganizationReference.link.rel',
        'educationOrganizationReferenceId',
    )
    student_cohort_year_normalize = pdMerge(
        left=student_cohort_year_normalize,
        right=student_cohort_year_normalize_descriptor,
        how='inner',
        leftOn=['id'],
        rightOn=['id'],
        suffixLeft=None,
        suffixRight=None
    )
    add_dataframe_column(
        student_cohort_year_normalize,
        [
            'schoolYear'
        ]
    )
    student_cohort_year_normalize = renameColumns(
        student_cohort_year_normalize,
        {
            'studentReference.studentUniqueId': 'StudentKey',
            'educationOrganizationReference.educationOrganizationId': 'LocalEducationAgencyId',
            'educationOrganizationReferenceId': 'localEducationAgencyReferenceId'
        }
    )
    # Filter by LEA
    student_cohort_year_normalize = (
        student_cohort_year_normalize[
            student_cohort_year_normalize['educationOrganizationReference.link.rel'] == EDUCATION_ORGANIZATION_FILTER
        ]
    )
    student_cohort_year_normalize['StudentKey'] = student_cohort_year_normalize['StudentKey'].astype(str)
    student_cohort_year_normalize['LocalEducationAgencyId'] = student_cohort_year_normalize['LocalEducationAgencyId'].astype(str)

    student_cohort_year_normalize['DemographicKey'] = (
        'CohortYear:'
        + student_cohort_year_normalize['schoolYear']
        + '-' + student_cohort_year_normalize['descriptorCodeValue']
    )
    student_cohort_year_normalize['StudentLocalEducationAgencyKey'] = (
        student_cohort_year_normalize['StudentKey']
        + '-' + student_cohort_year_normalize['LocalEducationAgencyId']
    )
    student_cohort_year_normalize['StudentSchoolDemographicBridgeKey'] = (
        student_cohort_year_normalize['DemographicKey']
        + '-' + student_cohort_year_normalize['StudentLocalEducationAgencyKey']
    )
    # Select needed columns.
    student_cohort_year_normalize = subset(student_cohort_year_normalize, [
        'localEducationAgencyReferenceId',
        'studentReferenceId',
        'LocalEducationAgencyId',
        'StudentKey',
        'StudentSchoolDemographicBridgeKey',
        'StudentLocalEducationAgencyKey',
        'DemographicKey',
    ])

    ############################
    # Schools
    ############################
    school_normalize = jsonNormalize(
        school_content,
        recordPath=None,
        meta=[
            'id',
            'localEducationAgencyReference.localEducationAgencyId',
            'localEducationAgencyReference.link.href',
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    get_reference_from_href(
        school_normalize,
        'localEducationAgencyReference.link.href',
        'localEducationAgencyReferenceId'
    )
    school_normalize = renameColumns(
        school_normalize,
        {
            'id': 'schoolReferenceId',
            'localEducationAgencyReference.localEducationAgencyId': 'LocalEducationAgencyId'
        }
    )
    # Select needed columns.
    school_normalize = subset(school_normalize, [
        'schoolReferenceId',
        'localEducationAgencyReferenceId',
        'LocalEducationAgencyId'
    ])
    ############################
    # studentSchoolAssociation
    ############################
    student_school_association_normalize = jsonNormalize(
        student_school_association_content,
        recordPath=None,
        meta=[
            'schoolReference.link.href',
            'studentReference.studentUniqueId',
            'studentReference.link.href',
            'exitWithdrawDate'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    get_reference_from_href(
        student_school_association_normalize,
        'schoolReference.link.href',
        'schoolReferenceId',
    )
    get_reference_from_href(
        student_school_association_normalize,
        'studentReference.link.href',
        'studentReferenceId',
    )
    student_school_association_normalize['exitWithdrawDateKey'] = to_datetime_key(student_school_association_normalize, 'exitWithdrawDate')
    student_school_association_normalize['dateKey'] = date.today().strftime('%Y%m%d')
    student_school_association_normalize = (
        student_school_association_normalize[
            student_school_association_normalize['exitWithdrawDateKey'] >= student_school_association_normalize['dateKey']
        ]
    )
    student_school_association_normalize = renameColumns(
        student_school_association_normalize,
        {
            'studentReference.studentUniqueId': 'StudentKey',
        }
    )
    student_school_association_normalize = pdMerge(
        left=student_school_association_normalize,
        right=school_normalize,
        how='inner',
        leftOn=['schoolReferenceId'],
        rightOn=['schoolReferenceId'],
        suffixLeft=None,
        suffixRight=None
    )
    student_school_association_normalize['StudentKey'] = student_school_association_normalize['StudentKey'].astype(str)
    student_school_association_normalize['LocalEducationAgencyId'] = (
        student_school_association_normalize['LocalEducationAgencyId'].astype(str)
    )
    # Select needed columns.
    student_school_association_normalize = subset(student_school_association_normalize, [
        'localEducationAgencyReferenceId',
        'studentReferenceId',
        'exitWithdrawDateKey'
    ])
    demographics_data_frame = (
        student_cohort_year_normalize
    )
    ############################
    # Get Demographics
    ############################
    for item in demographics_dictionary_list:
        result_demographics = (
            get_student_demographic(
                student_education_organization_association_content,
                item
            )
        )
        result_data_frame_demographics = pdMerge(
            left=result_demographics,
            right=student_school_association_normalize,
            how='inner',
            leftOn=[
                'localEducationAgencyReferenceId',
                'studentReferenceId'
            ],
            rightOn=[
                'localEducationAgencyReferenceId',
                'studentReferenceId'
            ],
            suffixLeft=None,
            suffixRight=None
        )
        if not result_demographics.empty:
            if demographics_data_frame.empty:
                demographics_data_frame = result_demographics
            else:
                demographics_data_frame = (
                    pd.concat(
                        [
                            demographics_data_frame,
                            result_data_frame_demographics
                        ]
                    )
                )
    result_data_frame = demographics_data_frame

    # Select needed columns.
    result_data_frame = subset(result_data_frame, [
        'StudentSchoolDemographicBridgeKey',
        'StudentLocalEducationAgencyKey',
        'DemographicKey'
    ])
    return result_data_frame


def get_student_demographic(content, item) -> pd.DataFrame:
    student_education_organization_association_content = content
    path = item['path']
    derived_path = item['derived_path'] if 'derived_path' in item else ''
    ############################
    # Demographic
    ############################
    student_demographic_normalize = jsonNormalize(
        student_education_organization_association_content,
        recordPath=None,
        meta=[
            'id',
            'educationOrganizationReference.educationOrganizationId',
            'educationOrganizationReference.link.rel',
            'studentReference.studentUniqueId',
            'studentReference.link.href',
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    get_reference_from_href(
        student_demographic_normalize,
        'studentReference.link.href',
        'studentReferenceId',
    )
    get_reference_from_href(
        student_demographic_normalize,
        'educationOrganizationReference.link.href',
        'educationOrganizationReferenceId',
    )
    student_demographic_normalize = renameColumns(
        student_demographic_normalize,
        {
            'studentReference.studentUniqueId': 'StudentKey',
            'educationOrganizationReference.educationOrganizationId': 'LocalEducationAgencyId',
            'educationOrganizationReferenceId': 'localEducationAgencyReferenceId'
        }
    )
    ############################
    # Demographic nested
    ############################
    student_demographic_descriptor_normalize = jsonNormalize(
        student_education_organization_association_content,
        recordPath=[path],
        meta=[
            'id',
        ],
        metaPrefix=None,
        recordPrefix='descriptor_',
        errors='ignore'
    )
    # Get Descriptor
    get_descriptor_code_value_from_uri(
        student_demographic_descriptor_normalize,
        f'descriptor_{item["descriptor"]}'
    )
    student_demographic_descriptor_normalize = renameColumns(
        student_demographic_descriptor_normalize,
        {
            f'descriptor_{item["descriptor"]}': 'descriptorCodeValue',
        }
    )
    addColumnIfNotExists(
        student_demographic_descriptor_normalize,
        'prefix',
        item['prefix']
    )
    student_demographic_descriptor_normalize = (
        student_demographic_descriptor_normalize[
            'descriptorCodeValue' in student_demographic_descriptor_normalize
            and student_demographic_descriptor_normalize['descriptorCodeValue'] != ''
        ]
    )
    # Periods
    if 'descriptor_periods' in student_demographic_descriptor_normalize:
        student_demographic_descriptor_normalize_periods = student_demographic_descriptor_normalize['descriptor_periods'].explode().apply(pd.Series)
        student_demographic_descriptor_normalize = pdMerge(
            left=student_demographic_descriptor_normalize,
            right=student_demographic_descriptor_normalize_periods,
            how='left',
            leftOn=[student_demographic_descriptor_normalize.index.get_level_values(0)],
            rightOn=[student_demographic_descriptor_normalize_periods.index.get_level_values(0)]
        )
        addColumnIfNotExists(
            student_demographic_descriptor_normalize,
            'endDate',
            '21003112'
        )
        student_demographic_descriptor_normalize['endDateKey'] = (
            to_datetime_key(
                student_demographic_descriptor_normalize,
                'endDate'
            )
        )
        student_demographic_descriptor_normalize['dateKey'] = date.today().strftime('%Y%m%d')
        student_demographic_descriptor_normalize = (
            student_demographic_descriptor_normalize[
                student_demographic_descriptor_normalize['endDateKey'] >= student_demographic_descriptor_normalize['dateKey']
            ]
        )
    if not student_demographic_descriptor_normalize.empty:
        if derived_path != '':
            student_demographic_descriptor_normalize_derived = student_demographic_descriptor_normalize[f'descriptor_{derived_path}'].explode().apply(pd.Series)
            student_demographic_descriptor_normalize_derived = pdMerge(
                left=student_demographic_descriptor_normalize_derived,
                right=student_demographic_descriptor_normalize,
                how='inner',
                leftOn=[student_demographic_descriptor_normalize_derived.index.get_level_values(0)],
                rightOn=[student_demographic_descriptor_normalize.index.get_level_values(0)]
            )
            # Get Descriptor
            get_descriptor_code_value_from_uri(
                student_demographic_descriptor_normalize_derived,
                item['derived_descriptor']
            )
            student_demographic_descriptor_normalize_derived = renameColumns(
                student_demographic_descriptor_normalize_derived,
                {
                    'descriptorCodeValue': 'oldDescriptorCodeValue',
                    'prefix': 'oldPrefix',
                    item['derived_descriptor']: 'descriptorCodeValue',
                }
            )
            addColumnIfNotExists(
                student_demographic_descriptor_normalize_derived,
                'prefix',
                item['derived_prefix']
            )
            student_demographic_descriptor_normalize_derived = subset(student_demographic_descriptor_normalize_derived, [
                'id',
                'descriptorCodeValue',
                'prefix'
            ])
            if not student_demographic_descriptor_normalize_derived.empty:
                student_demographic_descriptor_normalize = pd_concat([
                    student_demographic_descriptor_normalize,
                    student_demographic_descriptor_normalize_derived
                ])
    student_demographic_normalize = pdMerge(
        left=student_demographic_normalize,
        right=student_demographic_descriptor_normalize,
        how='inner',
        leftOn=['id'],
        rightOn=['id'],
        suffixLeft=None,
        suffixRight=None
    )
    # Filter by LEA
    student_demographic_normalize = (
        student_demographic_normalize[
            student_demographic_normalize['educationOrganizationReference.link.rel'] == EDUCATION_ORGANIZATION_FILTER
        ]
    )
    student_demographic_normalize['StudentKey'] = student_demographic_normalize['StudentKey'].astype(str)
    student_demographic_normalize['LocalEducationAgencyId'] = student_demographic_normalize['LocalEducationAgencyId'].astype(str)
    student_demographic_normalize['DemographicKey'] = (
        student_demographic_normalize['prefix']
        + ':' + student_demographic_normalize['descriptorCodeValue']
    )
    student_demographic_normalize['StudentLocalEducationAgencyKey'] = (
        student_demographic_normalize['StudentKey']
        + '-' + student_demographic_normalize['LocalEducationAgencyId']
    )
    student_demographic_normalize['StudentSchoolDemographicBridgeKey'] = (
        student_demographic_normalize['DemographicKey']
        + '-' + student_demographic_normalize['StudentLocalEducationAgencyKey']
    )
    # Select needed columns.
    student_demographic_normalize = subset(student_demographic_normalize, [
        'localEducationAgencyReferenceId',
        'studentReferenceId',
        'LocalEducationAgencyId',
        'StudentKey',
        'StudentSchoolDemographicBridgeKey',
        'StudentLocalEducationAgencyKey',
        'DemographicKey',
    ])
    return student_demographic_normalize


def student_local_education_agency_demographics_bridge(school_year) -> None:
    result_data_frame = student_local_education_agency_demographics_bridge_dataframe(school_year)
    saveParquetFile(result_data_frame, f"{config('PARQUET_FILES_LOCATION')}", "StudentLocalEducationAgencyDemographicsBridge.parquet", school_year)

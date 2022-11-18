# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from datetime import date

import pandas as pd
from decouple import config
from dagster import get_dagster_logger
from edfi_amt_data_lake.helper.data_frame_generation_result import (
    data_frame_generation_result,
)
from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    addColumnIfNotExists,
    create_parquet_file,
    get_descriptor_code_value_from_uri,
    get_reference_from_href,
    jsonNormalize,
    pd_concat,
    pdMerge,
    renameColumns,
    subset,
    to_datetime_key,
)

ENDPOINT_STUDENT_EDUCATION_ORGANIZATION_ASSOCIATION = 'studentEducationOrganizationAssociations'
ENDPOINT_STUDENT_SCHOOL_ASSOCIATION = 'studentSchoolAssociations'
RESULT_COLUMNS = [
    'StudentSchoolDemographicBridgeKey',
    'StudentSchoolKey',
    'DemographicKey'
]


@create_parquet_file
def student_school_demographics_bridge_data_frame(
    file_name: str,
    columns: list[str],
    school_year: int
) -> pd.DataFrame:
    parquet_logger = get_dagster_logger()
    parquet_logger.debug(f'Start with file: {file_name}')    
    student_school_association_content = getEndpointJson(ENDPOINT_STUDENT_SCHOOL_ASSOCIATION, config('SILVER_DATA_LOCATION'), school_year)
    student_education_organization_association_content = getEndpointJson(ENDPOINT_STUDENT_EDUCATION_ORGANIZATION_ASSOCIATION, config('SILVER_DATA_LOCATION'), school_year)
    demographics_dictionary_list = [
        {
            'prefix': 'CohortYear',
            'path': 'cohortYears',
            'descriptor': 'cohortYearTypeDescriptor',
        },
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
    # studentSchoolAssociation
    ############################
    student_school_association_normalize = jsonNormalize(
        student_school_association_content,
        recordPath=None,
        meta=[
            'schoolReference.schoolId',
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
    student_school_association_normalize = renameColumns(
        student_school_association_normalize,
        {
            'studentReference.studentUniqueId': 'StudentKey',
            'schoolReference.schoolId': 'SchoolKey',
        }
    )
    student_school_association_normalize['StudentKey'] = student_school_association_normalize['StudentKey'].astype(str)
    student_school_association_normalize['SchoolKey'] = student_school_association_normalize['SchoolKey'].astype(str)
    # Select needed columns.
    student_school_association_normalize = subset(student_school_association_normalize, [
        'SchoolKey',
        'StudentKey',
        'exitWithdrawDateKey',
        'dateKey'
    ])
    demographics_data_frame = pd.DataFrame()
    ############################
    # Get Demographics
    ############################
    for item in demographics_dictionary_list:
        parquet_logger.debug(f'Process student demographics: {item["prefix"]}')
        result_demographics = (
            get_student_demographic(
                student_education_organization_association_content,
                item
            )
        )
        if not result_demographics.empty:
            demographics_data_frame = (
                pd_concat(
                    [
                        demographics_data_frame, 
                        result_demographics,                                                       
                    ]
                )
            )
    if not (demographics_data_frame is None or demographics_data_frame.empty):
        result_data_frame = pdMerge(
            left=demographics_data_frame,
            right=student_school_association_normalize,
            how='left',
            leftOn=[
                'SchoolKey',
                'StudentKey'
            ],
            rightOn=[
                'SchoolKey',
                'StudentKey'
            ],
            suffixLeft=None,
            suffixRight=None
        )
        if not (result_data_frame is None and result_data_frame.empty):
            result_data_frame['StudentSchoolKey'] = (
                result_data_frame['StudentKey'].astype(str)
                + '-' + result_data_frame['SchoolKey'].astype(str)
            )
            result_data_frame['StudentSchoolDemographicBridgeKey'] = (
                result_data_frame['DemographicKey']
                + '-' + result_data_frame['StudentSchoolKey']
            )
            result_data_frame = (
                result_data_frame[
                result_data_frame['exitWithdrawDateKey'] >= result_data_frame['dateKey']
                ]
            )
        else:
            return None
    else:
        return None
    # Select needed columns.    
    result_data_frame = subset(result_data_frame, columns)    
    return result_data_frame


def get_student_demographic(content, item) -> pd.DataFrame:
    parquet_logger = get_dagster_logger()
    student_education_organization_association_content = content
    path = item['path']
    derived_path = item['derived_path'] if 'derived_path' in item else ''
    prefix = item['prefix']
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
            'educationOrganizationReference.educationOrganizationId': 'SchoolKey',
            'educationOrganizationReferenceId': 'schoolReferenceId'
        }
    )
    student_demographic_normalize = subset(
        student_demographic_normalize,
        [
            'id',
            'StudentKey',
            'SchoolKey',
            'schoolReferenceId',
            'studentReferenceId',
            'educationOrganizationReference.link.rel'
        ]
    )
    ############################
    # Demographic descriptor
    ############################
    student_demographic_descriptor_normalize = jsonNormalize(
        student_education_organization_association_content,
        recordPath=[path],
        meta=[
            'id',
        ],
        metaPrefix=None,
        recordMeta=[
            item["descriptor"],
            'periods',
            'periods.endDate',
            derived_path,
            'schoolYearTypeReference.schoolYear',
        ],
        recordPrefix='descriptor_',
        errors='ignore'
    )
    student_demographic_descriptor_normalize.loc[student_demographic_descriptor_normalize[f'descriptor_{item["descriptor"]}'].isnull(),f'descriptor_{item["descriptor"]}'] = '' 
    # Get Descriptor
    get_descriptor_code_value_from_uri(
        student_demographic_descriptor_normalize,
        f'descriptor_{item["descriptor"]}'
    )
    student_demographic_descriptor_normalize = renameColumns(
        student_demographic_descriptor_normalize,
        {
            f'descriptor_{item["descriptor"]}': 'descriptorCodeValue',
            'descriptor_schoolYearTypeReference.schoolYear': 'schoolYear'
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
        addColumnIfNotExists(
            student_demographic_descriptor_normalize,
            'descriptor_periods.endDate',
            ''
        )
        student_demographic_descriptor_normalize['descriptor_periods'] = student_demographic_descriptor_normalize['descriptor_periods.endDate'].explode().apply(pd.Series)
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
    ############################
    # Demographic descriptor (Derived)
    ############################
    if not student_demographic_descriptor_normalize.empty:
        if derived_path != '':
            derived_column = f'descriptor_{derived_path}'
            student_demographic_descriptor_normalize_derived = student_demographic_descriptor_normalize.copy()
            student_demographic_descriptor_normalize_derived[derived_column] =  student_demographic_descriptor_normalize[derived_column].explode().apply(pd.Series)
            student_demographic_descriptor_normalize_derived.loc[
                student_demographic_descriptor_normalize_derived[derived_column].isnull(),derived_column
            ] = '' 
            # Get Descriptor
            get_descriptor_code_value_from_uri(
                student_demographic_descriptor_normalize_derived,
                derived_column
            )
            student_demographic_descriptor_normalize_derived['descriptorCodeValue'] = (
                student_demographic_descriptor_normalize_derived[derived_column]
            )
            student_demographic_descriptor_normalize_derived['prefix'] = (
                item['derived_prefix']
            )
            student_demographic_descriptor_normalize_derived = subset(student_demographic_descriptor_normalize_derived, [
                'id',
                'descriptorCodeValue',
                'prefix'
            ])
            student_demographic_descriptor_normalize_derived = (
                student_demographic_descriptor_normalize_derived[
                    'descriptorCodeValue' in student_demographic_descriptor_normalize_derived
                    and student_demographic_descriptor_normalize_derived['descriptorCodeValue'] != ''
                ]
            )
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
    
    student_demographic_normalize['SchoolKey'] = student_demographic_normalize['SchoolKey'].astype(str)
    if prefix == 'CohortYear':
        student_demographic_normalize['DemographicKey'] = (
            student_demographic_normalize['prefix']
            + ':' + student_demographic_normalize['schoolYear'].astype(str)
            + '-' + student_demographic_normalize['descriptorCodeValue']
        )
    else:
        student_demographic_normalize['DemographicKey'] = (
            student_demographic_normalize['prefix']
            + ':' + student_demographic_normalize['descriptorCodeValue']
        )
    # Select needed columns.
    student_demographic_normalize = subset(student_demographic_normalize, [
        'SchoolKey',
        'StudentKey',
        'DemographicKey',
    ])
    return student_demographic_normalize


def student_school_demographics_bridge(school_year) -> data_frame_generation_result:
    return student_school_demographics_bridge_data_frame(
        file_name="studentSchoolDemographicsBridge.parquet",
        columns=RESULT_COLUMNS,
        school_year=school_year
    )

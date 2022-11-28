# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from datetime import date

import pandas as pd
from dagster import get_dagster_logger
from decouple import config

from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    copy_value_by_column,
    create_parquet_file,
    get_descriptor_code_value_from_uri,
    get_reference_from_href,
    jsonNormalize,
    pdMerge,
    renameColumns,
    replace_null,
    subset,
    to_datetime_key,
)

ENDPOINT_STUDENT_EDUCATION_ORGANIZATION_ASSOCIATION = 'studentEducationOrganizationAssociations'
ENDPOINT_STUDENT = 'students'
ENDPOINT_STUDENT_SCHOOL_ASSOCIATION = 'studentSchoolAssociations'
ENDPOINT_LOCAL_EDUCATION_AGENCY = 'localEducationAgencies'
EDUCATION_ORGANIZATION_FILTER = 'LocalEducationAgency'
RESULT_COLUMNS = [
    'StudentLocalEducationAgencyKey',
    'StudentKey',
    'LocalEducationAgencyKey',
    'StudentFirstName',
    'StudentMiddleName',
    'StudentLastName',
    'LimitedEnglishProficiency',
    'IsHispanic',
    'Sex',
    'InternetAccessInResidence',
    'InternetAccessTypeInResidence',
    'InternetPerformance',
    'DigitalDevice',
    'DeviceAccess'
]
INDICATOR_LIST_MAP = [
    {
        'source_column': 'Internet Access In Residence',
        'destination': 'InternetAccessInResidence'
    },
    {
        'source_column': 'Internet Access Type In Residence',
        'destination': 'InternetAccessTypeInResidence'
    },
    {
        'source_column': 'Internet Performance',
        'destination': 'InternetPerformance'
    },
    {
        'source_column': 'Digital Device',
        'destination': 'DigitalDevice'
    },
    {
        'source_column': 'Device Access',
        'destination': 'DeviceAccess'
    }
]


@create_parquet_file
def student_local_education_agency_dataframe(
    file_name: str,
    columns: list[str],
    school_year: int
) -> pd.DataFrame:
    parquet_logger = get_dagster_logger()
    parquet_logger.debug(f'Start with file: {file_name}') 
    student_education_organization_association_content = getEndpointJson(ENDPOINT_STUDENT_EDUCATION_ORGANIZATION_ASSOCIATION, config('SILVER_DATA_LOCATION'), school_year)
    student_content = getEndpointJson(ENDPOINT_STUDENT, config('SILVER_DATA_LOCATION'), school_year)
    student_school_association_content = getEndpointJson(ENDPOINT_STUDENT_SCHOOL_ASSOCIATION, config('SILVER_DATA_LOCATION'), school_year)
    local_education_agency_content = getEndpointJson(ENDPOINT_LOCAL_EDUCATION_AGENCY, config('SILVER_DATA_LOCATION'), school_year)
    ############################
    # STUDENT
    ############################
    student_normalize = jsonNormalize(
        student_content,
        recordPath=None,
        meta=[
            'id',
            'firstName',
            'studentUniqueId',
            'middleName',
            'lastSurname'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    student_normalize = renameColumns(student_normalize,
        {
            'id': 'studentReferenceId',
            'studentUniqueId': 'StudentKey',
            'firstName': 'StudentFirstName',
            'middleName': 'StudentMiddleName',
            'lastSurname': 'StudentLastName'
        }
    )
    replace_null(student_normalize, 'StudentMiddleName', '')
    student_normalize['StudentKey'] = (
        student_normalize['StudentKey'].astype(str)
    )
    ############################
    # student_school_association
    ############################
    student_school_association_normalize = jsonNormalize(
        student_school_association_content,
        recordPath=None,
        meta=[
            'id',
            'studentReference.link.href',
            'exitWithdrawDate'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    student_school_association_normalize['exitWithdrawDateKey'] = to_datetime_key(
        student_school_association_normalize,
        'exitWithdrawDate'
    )
    student_school_association_normalize['dateKey'] = date.today().strftime('%Y%m%d')
    get_reference_from_href(
        student_school_association_normalize,
        'studentReference.link.href',
        'studentReferenceId'
    )
    student_normalize = pdMerge(
        left=student_normalize,
        right=student_school_association_normalize,
        how='inner',
        leftOn=['studentReferenceId'],
        rightOn=['studentReferenceId'],
        suffixLeft='_student',
        suffixRight='_student_school_association'
    )
    if student_normalize is None: 
        return None
    student_normalize = (
            student_normalize[
                student_normalize['exitWithdrawDateKey'] >= student_normalize['dateKey']
            ]
        )
    ############################
    # LocalEducationAgency
    ############################
    local_education_agency_normalize = jsonNormalize(
        local_education_agency_content,
        recordPath=None,
        meta=[
            'id',
            'localEducationAgencyId',
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    local_education_agency_normalize = renameColumns(local_education_agency_normalize,
        {
            'id': 'localEducationAgencyReferenceId',
            'localEducationAgencyId': 'LocalEducationAgencyKey'
        }
    )
    local_education_agency_normalize['LocalEducationAgencyKey'] = (
        local_education_agency_normalize['LocalEducationAgencyKey'].astype(str)
    )
    ############################
    # student_education_organization_association
    ############################
    student_education_organization_association_normalize = jsonNormalize(
        student_education_organization_association_content,
        recordPath=None,
        meta=[
            'id',
            'studentReference.link.href',
            'educationOrganizationReference.link.href',
            'limitedEnglishProficiencyDescriptor',
            'hispanicLatinoEthnicity',
            'sexDescriptor'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    get_descriptor_code_value_from_uri(
        student_education_organization_association_normalize,
        'limitedEnglishProficiencyDescriptor'
    )
    get_descriptor_code_value_from_uri(
        student_education_organization_association_normalize,
        'sexDescriptor'
    )
    get_reference_from_href(
        student_education_organization_association_normalize,
        'studentReference.link.href',
        'studentReferenceId'
    )
    get_reference_from_href(
        student_education_organization_association_normalize,
        'educationOrganizationReference.link.href',
        'localEducationAgencyReferenceId'
    )
    replace_null(
        student_education_organization_association_normalize,
        'limitedEnglishProficiencyDescriptor',
        'Not Applicable'
    )
    replace_null(
        student_education_organization_association_normalize,
        'hispanicLatinoEthnicity',
        '0'
    )
    replace_null(
        student_education_organization_association_normalize,
        'sexDescriptor',
        ''
    )
    student_education_organization_association_normalize = renameColumns(
        student_education_organization_association_normalize,
        {
            'id': 'studentEducationOrganizationAssociationReferenceId',
            'limitedEnglishProficiencyDescriptor': 'LimitedEnglishProficiency',
            'hispanicLatinoEthnicity': 'IsHispanicBoolean',
            'sexDescriptor': 'Sex'
        }
    )
    ############################
    # student_education_organization_association
    ############################
    student_edorg_association_indicator_normalize = jsonNormalize(
        student_education_organization_association_content,
        recordPath='studentIndicators',
        meta=[
            'id',
        ],
        recordMeta=[
            'indicatorName',
            'indicator',
            'indicatorGroup'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    student_edorg_association_indicator_normalize = renameColumns(
        student_edorg_association_indicator_normalize,
        {
            'id': 'studentEducationOrganizationAssociationReferenceId'
        }
    )
    student_edorg_association_indicator_normalize = (
        student_edorg_association_indicator_normalize.pivot(
            index='studentEducationOrganizationAssociationReferenceId',
            columns='indicatorName',
            values='indicator'
         )
    )
    for item in INDICATOR_LIST_MAP:
        student_edorg_association_indicator_normalize[item['destination']] = (
            copy_value_by_column(
                student_edorg_association_indicator_normalize,
                item['source_column'],
                'n/a'
            )
        )
    ############################
    # student_education_organization_association
    ############################
    result_data_frame = pdMerge(
        left=student_education_organization_association_normalize,
        right=local_education_agency_normalize,
        how='inner',
        leftOn=['localEducationAgencyReferenceId'],
        rightOn=['localEducationAgencyReferenceId'],
        suffixLeft='_studentEdOrgAssociation',
        suffixRight='_local_education_agency'
    )
    if result_data_frame is None: 
        return None
    ############################
    # student_education_organization_association
    ############################
    result_data_frame = pdMerge(
        left=result_data_frame,
        right=student_edorg_association_indicator_normalize,
        how='left',
        leftOn=['studentEducationOrganizationAssociationReferenceId'],
        rightOn=['studentEducationOrganizationAssociationReferenceId'],
        suffixLeft='_studentEdOrgAssociation',
        suffixRight='_studentEdOrgAssociationIndicator'
    )
    if result_data_frame is None: 
        return None
    ############################
    # student_education_organization_association
    ############################
    result_data_frame = pdMerge(
        left=result_data_frame,
        right=student_normalize,
        how='inner',
        leftOn=['studentReferenceId'],
        rightOn=['studentReferenceId'],
        suffixLeft='_studentEdOrgAssociation',
        suffixRight='_student'
    )
    if result_data_frame is None: 
        return None
    
    result_data_frame['StudentLocalEducationAgencyKey'] = (
        result_data_frame['StudentKey']
        + '-' + result_data_frame['LocalEducationAgencyKey']
    )
    result_data_frame.loc[result_data_frame['IsHispanicBoolean'].astype(str).str.upper() == 'TRUE', 'IsHispanic'] = '1'
    result_data_frame.loc[result_data_frame['IsHispanicBoolean'].astype(str).str.upper() != 'TRUE', 'IsHispanic'] = '0'
    replace_null(result_data_frame, 'IsHispanic', '0')
    result_data_frame['IsHispanic'].astype(int)
    result_data_frame = subset(
        result_data_frame,
        columns
    )
    return result_data_frame


def student_local_education_agency_dim(school_year) -> None:
    return student_local_education_agency_dataframe(
        file_name="studentLocalEducationAgencyDim.parquet",
        columns=RESULT_COLUMNS,
        school_year=school_year
    )

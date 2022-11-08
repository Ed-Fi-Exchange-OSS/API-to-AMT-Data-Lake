# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from datetime import date

import pandas as pd
from decouple import config

from edfi_amt_data_lake.parquet.Common.descriptor_mapping import get_descriptor_constant
from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    addColumnIfNotExists,
    get_reference_from_href,
    jsonNormalize,
    pdMerge,
    renameColumns,
    saveParquetFile,
    subset,
    to_datetime_key,
)

ENDPOINT_STAFF_EDORG_ASSIGNMENT_ASSOCIATION = 'staffEducationOrganizationAssignmentAssociations'
ENDPOINT_STAFF_SECTION_ASSOCIATION = 'staffSectionAssociations'


def rls_user_authorization_dataframe(school_year) -> pd.DataFrame:
    staff_edorg_assignment_association_content = getEndpointJson(ENDPOINT_STAFF_EDORG_ASSIGNMENT_ASSOCIATION, config('SILVER_DATA_LOCATION'), school_year)
    staff_section_association_content = getEndpointJson(ENDPOINT_STAFF_SECTION_ASSOCIATION, config('SILVER_DATA_LOCATION'), school_year)
    ############################
    # staffEducationOrganizationAssignmentAssociations
    ############################
    staff_edorg_assignment_association_normalize = jsonNormalize(
        staff_edorg_assignment_association_content,
        recordPath=None,
        meta=[
            'studentReference.studentUniqueId',
            'staffReference.staffUniqueId',
            'staffClassificationDescriptor',
            'educationOrganizationReference.link.href',
            'educationOrganizationReference.educationOrganizationId'
            'endDate'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    staff_edorg_assignment_association_normalize = (
        get_descriptor_constant(staff_edorg_assignment_association_normalize, 'staffClassificationDescriptor')
    )
    staff_edorg_assignment_association_normalize = (
        staff_edorg_assignment_association_normalize[
            (staff_edorg_assignment_association_normalize['staffClassificationDescriptor_constantName'].str.contains('AuthorizationScope.District'))
            | (staff_edorg_assignment_association_normalize['staffClassificationDescriptor_constantName'].str.contains('AuthorizationScope.School'))
            | (staff_edorg_assignment_association_normalize['staffClassificationDescriptor_constantName'].str.contains('AuthorizationScope.Section'))
        ]
    )
    get_reference_from_href(
        staff_edorg_assignment_association_normalize,
        'educationOrganizationReference.link.href',
        'edOrgReferenceId'
    )
    get_reference_from_href(
        staff_edorg_assignment_association_normalize,
        'staffReference.link.href',
        'staffReferenceId'
    )
    staff_edorg_assignment_association_normalize = renameColumns(staff_edorg_assignment_association_normalize, {
        'staffReference.staffUniqueId': 'userKey',
        'educationOrganizationReference.educationOrganizationId': 'educationOrganizationId'
    })
    # Dates to validate endDate.
    addColumnIfNotExists(staff_edorg_assignment_association_normalize, 'endDate', '2199-12-31')
    staff_edorg_assignment_association_normalize['endDateKey'] = (
        to_datetime_key(staff_edorg_assignment_association_normalize, 'endDate')
    )
    staff_edorg_assignment_association_normalize['date_now'] = date.today()
    staff_edorg_assignment_association_normalize['date_now'] = (
        to_datetime_key(staff_edorg_assignment_association_normalize, 'date_now')
    )
    # Select needed columns.
    staff_edorg_assignment_association_normalize = subset(staff_edorg_assignment_association_normalize, [
        'userKey',
        'date_now',
        'endDateKey',
        'edOrgReferenceId',
        'staffClassificationDescriptor_constantName',
        'educationOrganizationId',
        'staffReferenceId',
    ])
    ############################
    # staff-section
    ############################
    staff_section_association_normalize = jsonNormalize(
        staff_section_association_content,
        recordPath=None,
        meta=[
            'id',
            'sectionReference.link.href',
            'staffReference.link.href',
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    get_reference_from_href(
        staff_section_association_normalize,
        'sectionReference.link.href',
        'sectionReferenceId',
    )
    get_reference_from_href(
        staff_section_association_normalize,
        'staffReference.link.href',
        'staffReferenceId',
    )
    # Select needed columns.
    staff_section_association_normalize = subset(staff_section_association_normalize, [
        'sectionReferenceId',
        'staffReferenceId',
        'sectionReference.schoolId',
        'sectionReference.localCourseCode',
        'sectionReference.schoolYear',
        'sectionReference.sectionIdentifier',
        'sectionReference.sessionName'
    ]).drop_duplicates()
    ############################
    # Section -> EdOrg = Section
    ############################
    result_section_data_frame = pdMerge(
        left=staff_edorg_assignment_association_normalize,
        right=staff_section_association_normalize,
        how='left',
        leftOn=['staffReferenceId'],
        rightOn=['staffReferenceId'],
        suffixLeft=None,
        suffixRight=None
    )
    result_section_data_frame['sectionReference.schoolKey'] = result_section_data_frame['sectionReference.schoolId'].astype('Int64').astype(str)
    result_section_data_frame['sectionReference.schoolYear'] = result_section_data_frame['sectionReference.schoolYear'].astype('Int64').astype(str)
    result_section_data_frame['sectionKey'] = (
        result_section_data_frame['sectionReference.schoolKey']
        + '-' + result_section_data_frame['sectionReference.localCourseCode']
        + '-' + result_section_data_frame['sectionReference.schoolYear']
        + '-' + result_section_data_frame['sectionReference.sectionIdentifier']
        + '-' + result_section_data_frame['sectionReference.sessionName']
    )
    result_section_data_frame = (
        result_section_data_frame[
            (
                result_section_data_frame['staffClassificationDescriptor_constantName'].str.contains('AuthorizationScope.District')
                | result_section_data_frame['staffClassificationDescriptor_constantName'].str.contains('AuthorizationScope.School')
                | result_section_data_frame['staffClassificationDescriptor_constantName'].str.contains('AuthorizationScope.Section')
            )
        ]
    )
    # Select needed columns.
    # Section permission
    result_section_data_frame.loc[
        (
            result_section_data_frame['staffClassificationDescriptor_constantName'].str.contains('AuthorizationScope.District')
            | result_section_data_frame['staffClassificationDescriptor_constantName'].str.contains('AuthorizationScope.School')
        )
        , 'sectionPermission'
    ] = 'ALL'
    result_section_data_frame.loc[
        (
            result_section_data_frame['staffClassificationDescriptor_constantName'].str.contains('AuthorizationScope.Section')
        )
        , 'sectionPermission'
    ] = result_section_data_frame['sectionReferenceId']
    # Section key permission
    result_section_data_frame.loc[
        (
            result_section_data_frame['staffClassificationDescriptor_constantName'].str.contains('AuthorizationScope.District')
            | result_section_data_frame['staffClassificationDescriptor_constantName'].str.contains('AuthorizationScope.School')
        )
        , 'sectionKeyPermission'
    ] = 'ALL'
    result_section_data_frame.loc[
        (
            result_section_data_frame['staffClassificationDescriptor_constantName'].str.contains('AuthorizationScope.Section')
        )
        , 'sectionKeyPermission'
    ] = result_section_data_frame['sectionKey']
    # School
    result_section_data_frame.loc[
        (
            result_section_data_frame['staffClassificationDescriptor_constantName'].str.contains('AuthorizationScope.District')
        )
        , 'schoolPermission'
    ] = 'ALL'
    result_section_data_frame.loc[
        (
            result_section_data_frame['staffClassificationDescriptor_constantName'].str.contains('AuthorizationScope.Section')
            | result_section_data_frame['staffClassificationDescriptor_constantName'].str.contains('AuthorizationScope.School')
        )
        , 'schoolPermission'
    ] = result_section_data_frame['educationOrganizationId'].astype(str)
    # districtId
    result_section_data_frame.loc[
        (
            result_section_data_frame['staffClassificationDescriptor_constantName'].str.contains('AuthorizationScope.District')
        )
        , 'districtId'
    ] = result_section_data_frame['educationOrganizationId'].astype(str)
    result_section_data_frame['studentPermission'] = 'ALL'
    result_section_data_frame = renameColumns(
        result_section_data_frame, {
            'staffClassificationDescriptor_constantName': 'userScope'
        }
    )
    result_data_frame = subset(result_section_data_frame, [
        'userKey',
        'userScope',
        'studentPermission',
        'sectionPermission',
        'sectionKeyPermission',
        'schoolPermission',
        'districtId'
    ]).drop_duplicates()
    return result_data_frame


def rls_user_authorization(school_year) -> None:
    result_data_frame = rls_user_authorization_dataframe(school_year)
    saveParquetFile(result_data_frame, f"{config('PARQUET_FILES_LOCATION')}", "rls_UserAuthorization.parquet", school_year)

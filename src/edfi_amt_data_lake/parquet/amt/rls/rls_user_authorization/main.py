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
    create_parquet_file,
    jsonNormalize,
    pdMerge,
    renameColumns,
    subset,
    to_datetime_key,
    toCsv
)

ENDPOINT_STAFF_EDORG_ASSIGNMENT_ASSOCIATION = 'staffEducationOrganizationAssignmentAssociations'
ENDPOINT_STAFF_SECTION_ASSOCIATION = 'staffSectionAssociations'
RESULT_COLUMNS = [
    'UserKey',
    'UserScope',
    'StudentPermission',
    'SectionPermission',
    'SectionKeyPermission',
    'SchoolPermission',
    'DistrictId'
]

@create_parquet_file
def rls_user_authorization_dataframe(
    file_name: str,
    columns: list[str],
    school_year: int
):
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
            'educationOrganizationReference.educationOrganizationId',
            'endDate'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    if staff_edorg_assignment_association_normalize.empty:
        return None

    staff_edorg_assignment_association_normalize = (
        get_descriptor_constant(staff_edorg_assignment_association_normalize, 'staffClassificationDescriptor')
    )
    staff_edorg_assignment_association_normalize = (
        staff_edorg_assignment_association_normalize[
            (staff_edorg_assignment_association_normalize['staffClassificationDescriptor_constantName'].str.contains('AuthorizationScope.District', na=False))
            | (staff_edorg_assignment_association_normalize['staffClassificationDescriptor_constantName'].str.contains('AuthorizationScope.School', na=False))
            | (staff_edorg_assignment_association_normalize['staffClassificationDescriptor_constantName'].str.contains('AuthorizationScope.Section', na=False))
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
        'staffReference.staffUniqueId': 'UserKey',
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
        'UserKey',
        'date_now',
        'endDateKey',
        'edOrgReferenceId',
        'staffClassificationDescriptor_constantName',
        'educationOrganizationId',
        'staffReferenceId',
    ])
    toCsv(staff_edorg_assignment_association_normalize, 'C:/temp/edfi/parquet/', 'staff_edorg_assignment_association_normalize.csv', '')
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
            'sectionReference.schoolId',
            'sectionReference.localCourseCode',
            'sectionReference.schoolYear',
            'sectionReference.sectionIdentifier',
            'sectionReference.sessionName'
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
    toCsv(staff_section_association_normalize, 'C:/temp/edfi/parquet/', 'staff_section_association_normalize.csv', '')
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
    toCsv(staff_edorg_assignment_association_normalize, 'C:/temp/edfi/parquet/', 'staff_edorg_assignment_association_normalize.csv', '')
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
                result_section_data_frame['staffClassificationDescriptor_constantName'].str.contains('AuthorizationScope.District', na=False)
                | result_section_data_frame['staffClassificationDescriptor_constantName'].str.contains('AuthorizationScope.School', na=False)
                | result_section_data_frame['staffClassificationDescriptor_constantName'].str.contains('AuthorizationScope.Section', na=False)
            )
        ]
    )
    # Select needed columns.
    # Section permission
    result_section_data_frame.loc[
        (
            result_section_data_frame['staffClassificationDescriptor_constantName'].str.contains('AuthorizationScope.District', na=False)
            | result_section_data_frame['staffClassificationDescriptor_constantName'].str.contains('AuthorizationScope.School', na=False)
        )
        , 'SectionPermission'
    ] = 'ALL'
    result_section_data_frame.loc[
        (
            result_section_data_frame['staffClassificationDescriptor_constantName'].str.contains('AuthorizationScope.Section', na=False)
        )
        , 'SectionPermission'
    ] = result_section_data_frame['sectionReferenceId']
    # Section key permission
    result_section_data_frame.loc[
        (
            result_section_data_frame['staffClassificationDescriptor_constantName'].str.contains('AuthorizationScope.District', na=False)
            | result_section_data_frame['staffClassificationDescriptor_constantName'].str.contains('AuthorizationScope.School', na=False)
        )
        , 'SectionKeyPermission'
    ] = 'ALL'
    result_section_data_frame.loc[
        (
            result_section_data_frame['staffClassificationDescriptor_constantName'].str.contains('AuthorizationScope.Section', na=False)
        )
        , 'SectionKeyPermission'
    ] = result_section_data_frame['sectionKey']
    # School
    result_section_data_frame.loc[
        (
            result_section_data_frame['staffClassificationDescriptor_constantName'].str.contains('AuthorizationScope.District', na=False)
        )
        , 'SchoolPermission'
    ] = 'ALL'
    result_section_data_frame.loc[
        (
            result_section_data_frame['staffClassificationDescriptor_constantName'].str.contains('AuthorizationScope.Section', na=False)
            | result_section_data_frame['staffClassificationDescriptor_constantName'].str.contains('AuthorizationScope.School', na=False)
        )
        , 'SchoolPermission'
    ] = result_section_data_frame['educationOrganizationId'].astype(str)
    # DistrictId
    result_section_data_frame.loc[
        (
            result_section_data_frame['staffClassificationDescriptor_constantName'].str.contains('AuthorizationScope.District', na=False)
        )
        , 'DistrictId'
    ] = result_section_data_frame['educationOrganizationId'].astype(str)
    result_section_data_frame['StudentPermission'] = 'ALL'
    result_section_data_frame = renameColumns(
        result_section_data_frame, {
            'staffClassificationDescriptor_constantName': 'UserScope'
        }
    )
    result_data_frame = subset(result_section_data_frame, columns).drop_duplicates()
    toCsv(result_data_frame, 'C:/temp/edfi/parquet/', 'rls_UserAuthorization.csv', '')
    return result_data_frame


def rls_user_authorization(school_year) -> None:
    return rls_user_authorization_dataframe(
        file_name="rls_UserAuthorization.parquet",
        columns=RESULT_COLUMNS,
        school_year=school_year
    )

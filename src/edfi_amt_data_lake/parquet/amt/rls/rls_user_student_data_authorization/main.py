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
    pd_concat,
    pdMerge,
    renameColumns,
    replace_null,
    saveParquetFile,
    subset,
    to_datetime_key,
)

ENDPOINT_STAFF_EDORG_ASSIGNMENT_ASSOCIATION = 'staffEducationOrganizationAssignmentAssociations'
ENDPOINT_STUDENT_SCHOOL_ASSOCIATION = 'studentSchoolAssociations'
ENDPOINT_SCHOOL = 'schools'
ENDPOINT_STUDENT_SECTION_ASSOCIATION = 'studentSectionAssociations'
ENDPOINT_STAFF_SECTION_ASSOCIATION = 'staffSectionAssociations'


def rls_user_student_data_authorization_dataframe(school_year) -> pd.DataFrame:
    staff_edorg_assignment_association_content = getEndpointJson(ENDPOINT_STAFF_EDORG_ASSIGNMENT_ASSOCIATION, config('SILVER_DATA_LOCATION'), school_year)
    student_school_association_content = getEndpointJson(ENDPOINT_STUDENT_SCHOOL_ASSOCIATION, config('SILVER_DATA_LOCATION'), school_year)
    school_content = getEndpointJson(ENDPOINT_SCHOOL, config('SILVER_DATA_LOCATION'), school_year)
    student_section_association_content = getEndpointJson(ENDPOINT_STUDENT_SECTION_ASSOCIATION, config('SILVER_DATA_LOCATION'), school_year)
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
        'staffReferenceId'
    ])
    ############################
    # school
    ############################
    school_normalize = jsonNormalize(
        school_content,
        recordPath=None,
        meta=[
            'id',
            'localEducationAgencyReference.link.href',
            'schoolId',
            'nameOfInstitution'
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
    school_normalize = renameColumns(school_normalize, {
        'id': 'schoolReferenceId',
        'schoolId': 'schoolKey',
        'nameOfInstitution': 'nameOfSchool'
    })
    # Select needed columns.
    school_normalize = subset(school_normalize, [
        'schoolReferenceId',
        'schoolKey',
        'nameOfSchool',
        'localEducationAgencyReferenceId'
    ])
    ############################
    # studentSchoolAssociations
    ############################
    student_school_association_normalize = jsonNormalize(
        student_school_association_content,
        recordPath=None,
        meta=[
            'studentReference.studentUniqueId',
            'schoolReference.link.href',
            'exitWithdrawDate'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    get_reference_from_href(
        student_school_association_normalize,
        'schoolReference.link.href',
        'schoolReferenceId'
    )
    replace_null(student_school_association_normalize, "exitWithdrawDate", '2199-12-31')

    student_school_association_normalize["exitWithdrawDateKey"] = (
        to_datetime_key(student_school_association_normalize, "exitWithdrawDate")
    )
    student_school_association_normalize = renameColumns(student_school_association_normalize, {
        'studentReference.studentUniqueId': 'studentKey',
        'schoolReference.schoolId': 'schoolKey'
    })
    # Select needed columns.
    student_school_association_normalize = subset(student_school_association_normalize, [
        'schoolReferenceId',
        'studentKey',
        'exitWithdrawDateKey',
        'schoolKey',
    ])
    ############################
    # section
    ############################
    student_section_association_normalize = jsonNormalize(
        student_section_association_content,
        recordPath=None,
        meta=[
            'studentReference.link.href'
            'studentReference.studentUniqueId',
            'sectionReference.schoolId',
            'endDate'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    get_reference_from_href(
        student_section_association_normalize,
        'studentReference.link.href',
        'studentReferenceId'
    )
    get_reference_from_href(
        student_section_association_normalize,
        'sectionReference.link.href',
        'sectionReferenceId'
    )
    addColumnIfNotExists(student_section_association_normalize, 'endDate', '21991231')
    student_section_association_normalize['sectionEndDateKey'] = (
        to_datetime_key(student_section_association_normalize, 'endDate')
    )
    student_section_association_normalize = renameColumns(student_section_association_normalize, {
        'studentReference.studentUniqueId': 'studentKey',
        'sectionReference.schoolId': 'schoolKey',
    })
    # Select needed columns.
    student_section_association_normalize = subset(student_section_association_normalize, [
        'studentKey',
        'schoolKey',
        'sectionEndDateKey',
        'sectionReferenceId'
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
            'schoolId',
            'nameOfInstitution'
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
    ]).drop_duplicates()
    ############################
    # District -> EdOrg = LEA
    ############################
    result_district_data_frame = renameColumns(staff_edorg_assignment_association_normalize, {
        'edOrgReferenceId': 'localEducationAgencyReferenceId',
    })
    result_district_data_frame.reset_index()
    result_district_data_frame = pdMerge(
        left=result_district_data_frame,
        right=school_normalize,
        how='inner',
        leftOn=['localEducationAgencyReferenceId'],
        rightOn=['localEducationAgencyReferenceId'],
        suffixLeft=None,
        suffixRight=None
    )
    result_district_data_frame = pdMerge(
        left=result_district_data_frame,
        right=student_school_association_normalize,
        how='inner',
        leftOn=['schoolReferenceId'],
        rightOn=['schoolReferenceId'],
        suffixLeft='district_',
        suffixRight="school_"
    )
    result_district_data_frame = (
        result_district_data_frame[
            (result_district_data_frame['staffClassificationDescriptor_constantName'].str.contains('AuthorizationScope.District'))
        ]
    )
    result_district_data_frame = (
        result_district_data_frame[result_district_data_frame['endDateKey'] >= result_district_data_frame['date_now']]
    )
    result_district_data_frame = (
        result_district_data_frame[result_district_data_frame['exitWithdrawDateKey'] >= result_district_data_frame['date_now']]
    )
    # Select needed columns.
    result_district_data_frame = subset(result_district_data_frame, [
        'userKey',
        'studentKey'
    ]).drop_duplicates()
    ############################
    # School -> EdOrg = School
    ############################
    result_school_data_frame = pdMerge(
        left=staff_edorg_assignment_association_normalize,
        right=school_normalize,
        how='inner',
        leftOn=['edOrgReferenceId'],
        rightOn=['schoolReferenceId'],
        suffixLeft=None,
        suffixRight=None
    )
    result_school_data_frame = pdMerge(
        left=result_school_data_frame,
        right=student_school_association_normalize,
        how='inner',
        leftOn=['schoolReferenceId', 'schoolKey'],
        rightOn=['schoolReferenceId', 'schoolKey'],
        suffixLeft=None,
        suffixRight=None
    )
    result_school_data_frame = (
        result_school_data_frame[
            (result_school_data_frame['staffClassificationDescriptor_constantName'].str.contains('AuthorizationScope.School'))
        ]
    )
    result_school_data_frame = (
        result_school_data_frame[result_school_data_frame['endDateKey'] >= result_school_data_frame['date_now']]
    )
    result_school_data_frame = (
        result_school_data_frame[result_school_data_frame['exitWithdrawDateKey'] >= result_school_data_frame['date_now']]
    )
    # Select needed columns.
    result_school_data_frame = subset(result_school_data_frame, [
        'userKey',
        'studentKey'
    ]).drop_duplicates()
    ############################
    # Section -> EdOrg = Section
    ############################
    result_section_data_frame = pdMerge(
        left=staff_edorg_assignment_association_normalize,
        right=staff_section_association_normalize,
        how='inner',
        leftOn=['staffReferenceId'],
        rightOn=['staffReferenceId'],
        suffixLeft=None,
        suffixRight=None
    )
    result_section_data_frame = pdMerge(
        left=result_section_data_frame,
        right=student_section_association_normalize,
        how='inner',
        leftOn=[
            'educationOrganizationId',
            'sectionReferenceId'
        ],
        rightOn=[
            'schoolKey',
            'sectionReferenceId'
        ],
        suffixLeft=None,
        suffixRight=None
    )
    result_section_data_frame = pdMerge(
        left=result_section_data_frame,
        right=student_school_association_normalize,
        how='inner',
        leftOn=[
            'schoolKey',
            'studentKey'
        ],
        rightOn=[
            'schoolKey',
            'studentKey'
        ],
        suffixLeft=None,
        suffixRight=None
    )
    result_section_data_frame = (
        result_section_data_frame[
            (result_section_data_frame['staffClassificationDescriptor_constantName'].str.contains('AuthorizationScope.Section'))
        ]
    )
    result_section_data_frame = (
        result_section_data_frame[result_section_data_frame['sectionEndDateKey'] >= result_section_data_frame['date_now']]
    )
    result_section_data_frame = (
        result_section_data_frame[result_section_data_frame['exitWithdrawDateKey'] >= result_section_data_frame['date_now']]
    )
    # Select needed columns.
    result_section_data_frame = subset(result_section_data_frame, [
        'userKey',
        'studentKey'
    ]).drop_duplicates()
    ############################
    # CONCAT RESULTS
    ############################
    result_data_frame = pd_concat(
        [
            result_district_data_frame,
            result_section_data_frame,
            result_school_data_frame,
        ],
    )
    # Select needed columns.
    result_data_frame = subset(result_data_frame, [
        'userKey',
        'studentKey'
    ]).drop_duplicates()
    return result_data_frame


def rls_user_student_data_authorization(school_year) -> None:
    result_data_frame = rls_user_student_data_authorization_dataframe(school_year)
    saveParquetFile(result_data_frame, f"{config('PARQUET_FILES_LOCATION')}", "rls_UserStudentDataAuthorization.parquet", school_year)

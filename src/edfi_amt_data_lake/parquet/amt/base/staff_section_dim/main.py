# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import pandas as pd
from decouple import config

from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    addColumnIfNotExists,
    create_parquet_file,
    get_descriptor_code_value_from_uri,
    get_reference_from_href,
    jsonNormalize,
    pdMerge,
    renameColumns,
    subset,
    to_datetime_key,
    toCsv,
)

ENDPOINT_STAFF_SECTION_ASSOCIATIONS = 'staffSectionAssociations'
ENDPOINT_STAFF = 'staffs'
RESULT_COLUMNS = [
    'StaffSectionKey',
    'UserKey',
    'SchoolKey',
    'SectionKey',
    'PersonalTitlePrefix',
    'StaffFirstName',
    'StaffMiddleName',
    'StaffLastName',
    'ElectronicMailAddress',
    'Sex',
    'BirthDate',
    'Race',
    'HispanicLatinoEthnicity',
    'HighestCompletedLevelOfEducation',
    'YearsOfPriorProfessionalExperience',
    'YearsOfPriorTeachingExperience',
    'HighlyQualifiedTeacher',
    'LoginId'
]


@create_parquet_file
def staff_section_dim_dataframe(
    file_name: str,
    columns: list[str],
    school_year: int
) -> pd.DataFrame:
    staff_section_association_content = getEndpointJson(ENDPOINT_STAFF_SECTION_ASSOCIATIONS, config('SILVER_DATA_LOCATION'), school_year)
    staff_content = getEndpointJson(ENDPOINT_STAFF, config('SILVER_DATA_LOCATION'), school_year)
    
    staff_section_association_normalized = jsonNormalize(
        staff_section_association_content,
        recordPath=None,
        meta=[
            'id',
            ['staffReference','staffUniqueId'],
            ['staffReference','link','href'],
            ['sectionReference','schoolId'],
            ['sectionReference','localCourseCode'],
            ['sectionReference','schoolYear'],
            ['sectionReference','sectionIdentifier'],
            ['sectionReference','sessionName']
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    if staff_section_association_normalized.empty:
        return None

    get_reference_from_href(
        staff_section_association_normalized,
        'staffReference.link.href',
        'staffReferenceId',
    )
    
    staff_normalized = jsonNormalize(
        staff_content,
        recordPath=None,
        meta=[
            'id',
            'staffUniqueId',
            'personalTitlePrefix',
            'firstName',
            'middleName',
            'lastSurname',
            'birthDate',
            'sexDescriptor',
            'hispanicLatinoEthnicity',
            'highestCompletedLevelOfEducationDescriptor',
            'yearsOfPriorProfessionalExperience',
            'yearsOfPriorTeachingExperience',
            'highlyQualifiedTeacher',
            'loginId'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    get_descriptor_code_value_from_uri(staff_normalized, 'highestCompletedLevelOfEducationDescriptor')
    get_descriptor_code_value_from_uri(staff_normalized, 'sexDescriptor')

    staff_electronic_mails_normalized = jsonNormalize(
        staff_content,
        recordPath=['electronicMails'],
        meta=['id'],
        recordMeta=['electronicMailAddress'],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    if not staff_electronic_mails_normalized.empty:
        staff_normalized = pdMerge(
            left=staff_normalized,
            right=staff_electronic_mails_normalized,
            how='left',
            leftOn=['id'],
            rightOn=['id'],
            suffixLeft=None,
            suffixRight=None
        )

    addColumnIfNotExists(staff_normalized, 'electronicMailAddress', '')
    
    toCsv(staff_normalized, 'C:/temp/edfi/parquet/', 'staff_normalized.csv', '')
    toCsv(staff_electronic_mails_normalized, 'C:/temp/edfi/parquet/', 'staff_electronic_mails_normalized.csv', '')
    toCsv(staff_section_association_normalized, 'C:/temp/edfi/parquet/', 'staff_section_association_normalized.csv', '')
    
    result_data_frame = pdMerge(
        left=staff_section_association_normalized,
        right=staff_normalized,
        how='inner',
        leftOn=['staffReference.staffUniqueId'],
        rightOn=['staffUniqueId'],
        suffixLeft=None,
        suffixRight='_staff'
    )

    result_data_frame['StaffSectionKey'] = (
        result_data_frame['staffReference.staffUniqueId'] + '-' +
        result_data_frame['sectionReference.schoolId'].astype(str) + '-' +
        result_data_frame['sectionReference.localCourseCode'] + '-' +
        result_data_frame['sectionReference.schoolYear'].astype(str) + '-' +
        result_data_frame['sectionReference.sectionIdentifier'] + '-' +
        result_data_frame['sectionReference.sessionName']
    )

    result_data_frame['SectionKey'] = (
        result_data_frame['sectionReference.schoolId'].astype(str) + '-' +
        result_data_frame['sectionReference.localCourseCode'] + '-' +
        result_data_frame['sectionReference.schoolYear'].astype(str) + '-' +
        result_data_frame['sectionReference.sectionIdentifier'] + '-' +
        result_data_frame['sectionReference.sessionName']
    )

    result_data_frame = subset(result_data_frame, [
        'StaffSectionKey',
        'sectionReference.schoolId',
        'SectionKey',
        'personalTitlePrefix',
        'firstName',
        'middleName',
        'lastSurname',
        'electronicMailAddress',
        'sexDescriptor',
        'birthDate',
        'hispanicLatinoEthnicity',
        'highestCompletedLevelOfEducationDescriptor',
        'yearsOfPriorProfessionalExperience',
        'yearsOfPriorTeachingExperience',
        'highlyQualifiedTeacher',
        'loginId'
    ])

    result_data_frame["highlyQualifiedTeacher"] = result_data_frame["highlyQualifiedTeacher"].astype(int)

    toCsv(result_data_frame, 'C:/temp/edfi/parquet/', 'result_data_frame.csv', '')

    # # Select columns
    # result_data_frame = result_data_frame[
    #     columns
    # ]
    return result_data_frame


def staff_section_dim(school_year) -> None:
    return staff_section_dim_dataframe(
        file_name="staffSectionDim.parquet",
        columns=RESULT_COLUMNS,
        school_year=school_year
    )

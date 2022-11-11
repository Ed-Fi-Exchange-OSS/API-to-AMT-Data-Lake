# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import pandas as pd
from decouple import config

from edfi_amt_data_lake.helper.data_frame_generation_result import (
    data_frame_generation_result,
)
from edfi_amt_data_lake.parquet.Common.descriptor_mapping import get_descriptor_constant
from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    addColumnIfNotExists,
    create_parquet_file,
    get_descriptor_code_value_from_uri,
    jsonNormalize,
    pdMerge,
    renameColumns,
    subset,
    create_empty_data_frame,
    toCsv,
)

ENDPOINT_STUDENT_SCHOOL_ASSOCIATIONS = 'studentSchoolAssociations'
ENDPOINT_STUDENT_EDUCATION_ORGANIZATION_ASSOCIATIONS = 'studentEducationOrganizationAssociations'
ENDPOINT_STUDENTS = 'students'
ENDPOINT_SCHOOLS = 'schools'

RESULT_COLUMNS = [
    'StudentSchoolKey',
    'StudentKey',
    'SchoolKey',
    'SchoolYear',
    'StudentFirstName',
    'StudentMiddleName',
    'StudentLastName',
    'BirthDate',
    'EnrollmentDateKey',
    'GradeLevel',
    'LimitedEnglishProficiency',
    'IsHispanic',
    'Sex',
    'InternetAccessInResidence',
    'InternetAccessTypeInResidence',
    'InternetPerformance',
    'DigitalDevice',
    'DeviceAccess'
]


@create_parquet_file
def student_school_dim_data_frame(
    file_name: str,
    columns: list[str],
    school_year: int
) -> pd.DataFrame:
    file_name = file_name
    student_school_associations_content = getEndpointJson(ENDPOINT_STUDENT_SCHOOL_ASSOCIATIONS, config('SILVER_DATA_LOCATION'), school_year)
    student_school_education_organization_associations_content = getEndpointJson(ENDPOINT_STUDENT_EDUCATION_ORGANIZATION_ASSOCIATIONS, config('SILVER_DATA_LOCATION'), school_year)
    students_content = getEndpointJson(ENDPOINT_STUDENTS, config('SILVER_DATA_LOCATION'), school_year)
    schools_content = getEndpointJson(ENDPOINT_SCHOOLS, config('SILVER_DATA_LOCATION'), school_year)

    student_school_associations_normalized = jsonNormalize(
        data=student_school_associations_content,
        recordPath=None,
        meta=[
            'id',
            ['schoolReference', 'schoolId'],
            ['studentReference', 'studentUniqueId'],
            'entryDate',
            'entryGradeLevelDescriptor',
            'exitWithdrawDate'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    toCsv(student_school_associations_normalized, 'C:/temp/edfi/parquet/', 'student_school_associations_normalized.csv', '')

    # if student_school_associations_normalized.empty:
    #     return None

    get_descriptor_code_value_from_uri(student_school_associations_normalized, 'entryGradeLevelDescriptor')

    students_normalized = jsonNormalize(
        data=students_content,
        recordPath=None,
        meta=[
            'id',
            'studentUniqueId',
            'birthDate',
            'firstName',
            'lastSurname',
            'middleName'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    toCsv(students_normalized, 'C:/temp/edfi/parquet/', 'students_normalized.csv', '')
    
    schools_normalized = jsonNormalize(
        data=schools_content,
        recordPath=None,
        meta=[
            'id',
            'schoolId',
            ['localEducationAgencyReference', 'localEducationAgencyId']
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    toCsv(schools_normalized, 'C:/temp/edfi/parquet/', 'schools_normalized.csv', '')
    
    # Student Education Organization
    student_school_education_organization_associations_normalized = jsonNormalize(
        data=student_school_education_organization_associations_content,
        recordPath=None,
        meta=[
            'id',
            ['educationOrganizationReference', 'educationOrganizationId'],
            ['studentReference', 'studentUniqueId'],
            'hispanicLatinoEthnicity',
            'limitedEnglishProficiencyDescriptor',
            'sexDescriptor'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    get_descriptor_code_value_from_uri(student_school_education_organization_associations_normalized, 'limitedEnglishProficiencyDescriptor')
    get_descriptor_code_value_from_uri(student_school_education_organization_associations_normalized, 'sexDescriptor')

    student_school_education_organization_associations_indicators_normalized = jsonNormalize(
        data=student_school_education_organization_associations_content,
        recordPath=[
            'studentIndicators'
        ],
        meta=[
            'id'
        ],
        recordMeta=[
            'indicatorName'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    toCsv(student_school_education_organization_associations_indicators_normalized, 'C:/temp/edfi/parquet/', 'student_school_education_organization_associations_indicators_normalized.csv', '')
    
    student_school_education_organization_associations_normalized = pdMerge(
        left=student_school_education_organization_associations_normalized,
        right=student_school_education_organization_associations_indicators_normalized,
        how='left',
        leftOn=['id'],
        rightOn=['id'],
        suffixLeft='',
        suffixRight='_indicators'
    )
    toCsv(student_school_education_organization_associations_normalized, 'C:/temp/edfi/parquet/', 'student_school_education_organization_associations_normalized.csv', '')

    # Student Education Organization ends

    # District Education Organization 
    
    student_school_education_organization_associations_district_normalized = jsonNormalize(
        data=student_school_education_organization_associations_content,
        recordPath=None,
        meta=[
            'id',
            ['educationOrganizationReference', 'educationOrganizationId'],
            ['studentReference', 'studentUniqueId'],
            'hispanicLatinoEthnicity',
            'limitedEnglishProficiencyDescriptor',
            'sexDescriptor'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    get_descriptor_code_value_from_uri(student_school_education_organization_associations_district_normalized, 'limitedEnglishProficiencyDescriptor')
    get_descriptor_code_value_from_uri(student_school_education_organization_associations_district_normalized, 'sexDescriptor')

    # District Education Organization ends

    result_data_frame = pdMerge(
        left=student_school_associations_normalized,
        right=students_normalized,
        how='inner',
        leftOn=['studentReference.studentUniqueId'],
        rightOn=['studentUniqueId'],
        suffixLeft='',
        suffixRight='_students'
    )

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=schools_normalized,
        how='inner',
        leftOn=['schoolReference.schoolId'],
        rightOn=['schoolId'],
        suffixLeft='',
        suffixRight='_schools'
    )

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=student_school_education_organization_associations_normalized,
        how='left',
        leftOn=['studentReference.studentUniqueId', 'schoolReference.schoolId'],
        rightOn=['studentReference.studentUniqueId', 'educationOrganizationReference.educationOrganizationId'],
        suffixLeft='',
        suffixRight='_studentEdOrg'
    )
    toCsv(result_data_frame, 'C:/temp/edfi/parquet/', 'result_data_frame_indicators.csv', '')

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=student_school_education_organization_associations_district_normalized,
        how='left',
        leftOn=['studentReference.studentUniqueId', 'localEducationAgencyReference.localEducationAgencyId'],
        rightOn=['studentReference.studentUniqueId', 'educationOrganizationReference.educationOrganizationId'],
        suffixLeft='',
        suffixRight='_districtEdOrg'
    )

    result_data_frame = subset(result_data_frame, [
        'schoolReference.schoolId', 
        'studentReference.studentUniqueId',
        'entryDate',
        'entryGradeLevelDescriptor',
        'exitWithdrawDate',
        'birthDate',
        'firstName',
        'lastSurname',
        'middleName',
        'hispanicLatinoEthnicity',
        'limitedEnglishProficiencyDescriptor',
        'sexDescriptor',
        'indicatorName',
        'hispanicLatinoEthnicity_districtEdOrg',
        'limitedEnglishProficiencyDescriptor_districtEdOrg',
        'sexDescriptor_districtEdOrg'
    ])

    result_data_frame.fillna('')

    # LimitedEnglishProficiency
    result_data_frame['LimitedEnglishProficiency'] = (
        result_data_frame.apply(
            lambda x: x['limitedEnglishProficiencyDescriptor'] if x['limitedEnglishProficiencyDescriptor'] != '' else x['limitedEnglishProficiencyDescriptor_districtEdOrg'], axis=1
        )
    )
    
    # IsHispanic
    result_data_frame['IsHispanic'] = (
        result_data_frame.apply(
            lambda x: x['hispanicLatinoEthnicity'] if x['hispanicLatinoEthnicity'] != '' else x['hispanicLatinoEthnicity_districtEdOrg'], axis=1
        )
    )
    
    # Sex
    result_data_frame['Sex'] = (
        result_data_frame.apply(
            lambda x: x['sexDescriptor'] if x['sexDescriptor'] != '' else x['sexDescriptor_districtEdOrg'], axis=1
        )
    )
    toCsv(result_data_frame, 'C:/temp/edfi/parquet/', 'result_data_frame.csv', '')
    
    return result_data_frame[
        columns
    ]


def student_school_dim(school_year) -> data_frame_generation_result:
    return student_school_dim_data_frame(
        file_name="studentSchoolDim.parquet",
        columns=RESULT_COLUMNS,
        school_year=school_year
    )

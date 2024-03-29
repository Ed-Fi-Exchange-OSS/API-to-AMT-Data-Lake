# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from decouple import config

from edfi_amt_data_lake.parquet.Common.descriptor_mapping import get_descriptor_constant
from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    addColumnIfNotExists,
    create_parquet_file,
    createDataFrame,
    get_descriptor_code_value_from_uri,
    jsonNormalize,
    pdMerge,
    renameColumns,
    replace_null,
    subset,
    toDateTime,
)

RESULT_COLUMNS = [
    'StudentKey',
    'SchoolKey',
    'GradingPeriodKey',
    'StudentSectionKey',
    'SectionKey',
    'NumericGradeEarned',
    'LetterGradeEarned',
    'GradeType'

]
ENDPOINT_GRADES = 'grades'
GRADING_PERIOD = 'gradingPeriods'
GRADING_PERIOD_DESCRIPTOR_GRADES = 'gradingPeriodDescriptors'


@create_parquet_file
def student_section_grade_fact_data_frame(
    file_name: str,
    columns: list[str],
    school_year: int
):
    grades_content = getEndpointJson(ENDPOINT_GRADES, config('SILVER_DATA_LOCATION'), school_year)
    grading_periods_content = getEndpointJson(GRADING_PERIOD, config('SILVER_DATA_LOCATION'), school_year)
    grading_period_descriptor_content = getEndpointJson(GRADING_PERIOD_DESCRIPTOR_GRADES, config('SILVER_DATA_LOCATION'), school_year)
    file_name = file_name
    letter_grade_translation = createDataFrame(
        data=[
            ['A', 95],
            ['B', 85],
            ['C', 75],
            ['D', 65],
            ['F', 55]
        ],
        columns=['LetterGradeEarned', 'NumericGradeEarnedJoin'])
    ############################
    # grading_period_descriptor
    ############################
    grading_period_descriptor_normalized = jsonNormalize(
        grading_period_descriptor_content,
        recordPath=None,
        meta=[
            'gradingPeriodDescriptorId',
            'codeValue',
            'description',
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors="ignore",
    )
    grading_period_descriptor_normalized = renameColumns(
        grading_period_descriptor_normalized,
        {
            'codeValue': 'gradingPeriodDescriptorCodeValue',
            'description': 'gradingPeriodDescriptorDescription',
        }
    )
    grades_content_normalized = jsonNormalize(
        grades_content,
        recordPath=None,
        meta=[
            'gradingPeriodReference.gradingPeriodDescriptor',
            'gradingPeriodReference.periodSequence',
            'gradingPeriodReference.schoolId',
            'gradingPeriodReference.schoolYear',
            'letterGradeEarned',
            'numericGradeEarned',
            'studentSectionAssociationReference.studentUniqueId',
            'studentSectionAssociationReference.schoolId',
            'studentSectionAssociationReference.beginDate',
            'studentSectionAssociationReference.localCourseCode',
            'studentSectionAssociationReference.schoolYear',
            'studentSectionAssociationReference.sectionIdentifier',
            'studentSectionAssociationReference.sessionName',
            'gradeTypeDescriptor'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    grading_periods_content_normalized = jsonNormalize(
        grading_periods_content,
        recordPath=None,
        meta=[
            'gradingPeriodDescriptor',
            'periodSequence',
            'schoolReference.schoolId',
            'schoolYearTypeReference.schoolYear',
            'beginDate',
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    result_data_frame = pdMerge(
        left=grades_content_normalized,
        right=grading_periods_content_normalized,
        how='left',
        leftOn=[
            'gradingPeriodReference.gradingPeriodDescriptor',
            'gradingPeriodReference.periodSequence',
            'gradingPeriodReference.schoolId',
            'gradingPeriodReference.schoolYear'
        ],
        rightOn=[
            'gradingPeriodDescriptor',
            'periodSequence',
            'schoolReference.schoolId',
            'schoolYearTypeReference.schoolYear'
        ],
        suffixLeft='',
        suffixRight='_gradingPeriods'
    )
    # if result_data_frame is None:
    #     return None
    addColumnIfNotExists(result_data_frame, 'letterGradeEarned')

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=letter_grade_translation,
        how='left',
        leftOn=['letterGradeEarned'],
        rightOn=['LetterGradeEarned'],
        suffixLeft=None,
        suffixRight=None
    )

    result_data_frame.numericGradeEarned[result_data_frame.numericGradeEarned == 0] = result_data_frame.NumericGradeEarnedJoin

    # Removes namespace from Grading Period Descriptor
    get_descriptor_code_value_from_uri(result_data_frame, 'gradingPeriodReference.gradingPeriodDescriptor')
    result_data_frame = pdMerge(
        left=result_data_frame,
        right=grading_period_descriptor_normalized,
        how='inner',
        leftOn=[
            'gradingPeriodReference.gradingPeriodDescriptor',
        ],
        rightOn=[
            'gradingPeriodDescriptorCodeValue',
        ],
        suffixLeft='',
        suffixRight='_gradingPeriodDescriptor'
    )
    # Keep the fields I actually need.
    result_data_frame = subset(result_data_frame, [
        'studentSectionAssociationReference.studentUniqueId',
        'studentSectionAssociationReference.schoolId',
        'gradingPeriodReference.gradingPeriodDescriptor',
        'studentSectionAssociationReference.beginDate',
        'studentSectionAssociationReference.localCourseCode',
        'studentSectionAssociationReference.schoolYear',
        'studentSectionAssociationReference.sectionIdentifier',
        'studentSectionAssociationReference.sessionName',
        'numericGradeEarned',
        'letterGradeEarned',
        'gradeTypeDescriptor',
        'gradingPeriodDescriptorId',
        'beginDate',
    ])
    result_data_frame = get_descriptor_constant(result_data_frame, 'gradeTypeDescriptor')
    # Formatting begin date that will be used as part of the keys later
    result_data_frame['studentSectionAssociationReference.beginDate'] = toDateTime(result_data_frame['studentSectionAssociationReference.beginDate'])
    result_data_frame['studentSectionAssociationReference.beginDate'] = result_data_frame['studentSectionAssociationReference.beginDate'].dt.strftime('%Y%m%d')
    result_data_frame['beginDate'] = toDateTime(result_data_frame['beginDate'])
    result_data_frame['beginDate'] = result_data_frame['beginDate'].dt.strftime('%Y%m%d')
    # Removes namespace from Grade Type Descriptor
    get_descriptor_code_value_from_uri(result_data_frame, 'gradeTypeDescriptor')

    # Converting some fields to str as preparation for the parquet file.
    result_data_frame['studentSectionAssociationReference.schoolId'] = result_data_frame['studentSectionAssociationReference.schoolId'].astype(str)
    result_data_frame['studentSectionAssociationReference.schoolYear'] = result_data_frame['studentSectionAssociationReference.schoolYear'].astype(str)

    # Creates concatenation for GradingPeriodKey field
    result_data_frame['GradingPeriodKey'] = (
        result_data_frame['gradingPeriodDescriptorId'].astype(str)
        + '-' + result_data_frame['studentSectionAssociationReference.schoolId']
        + '-' + result_data_frame['beginDate']
    )

    # Creates concatenation for StudentSectionKey field
    result_data_frame['StudentSectionKey'] = (
        result_data_frame['studentSectionAssociationReference.studentUniqueId']
        + '-' + result_data_frame['studentSectionAssociationReference.schoolId']
        + '-' + result_data_frame['studentSectionAssociationReference.localCourseCode']
        + '-' + result_data_frame['studentSectionAssociationReference.schoolYear']
        + '-' + result_data_frame['studentSectionAssociationReference.sectionIdentifier']
        + '-' + result_data_frame['studentSectionAssociationReference.sessionName']
        + '-' + result_data_frame['studentSectionAssociationReference.beginDate']
    )

    # Creates concatenation for SectionKey field
    result_data_frame['SectionKey'] = (
        result_data_frame['studentSectionAssociationReference.schoolId']
        + '-' + result_data_frame['studentSectionAssociationReference.localCourseCode']
        + '-' + result_data_frame['studentSectionAssociationReference.schoolYear']
        + '-' + result_data_frame['studentSectionAssociationReference.sectionIdentifier']
        + '-' + result_data_frame['studentSectionAssociationReference.sessionName']
    )

    replace_null(result_data_frame, 'letterGradeEarned', '')

    # Rename columns to match AMT
    result_data_frame = renameColumns(result_data_frame, {
        'studentSectionAssociationReference.studentUniqueId': 'StudentKey',
        'studentSectionAssociationReference.schoolId': 'SchoolKey',
        'numericGradeEarned': 'NumericGradeEarned',
        'letterGradeEarned': 'LetterGradeEarned',
        'gradeTypeDescriptor': 'GradeType'
    })

    result_data_frame = (
        result_data_frame[(result_data_frame['gradeTypeDescriptor_constantName'].str.contains('GradeType.GradingPeriod', na=False)) | (result_data_frame['gradeTypeDescriptor_constantName'].str.contains('GradeType.Semester', na=False)) | (result_data_frame['gradeTypeDescriptor_constantName'].str.contains('GradeType.Final', na=False))]
    )
    # Reorder columns to match AMT
    result_data_frame = result_data_frame[columns]

    return result_data_frame


def student_section_grade_fact(school_year: int):
    return student_section_grade_fact_data_frame(
        file_name="ews_studentSectionGradeFact.parquet",
        columns=RESULT_COLUMNS,
        school_year=school_year
    )

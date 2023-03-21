# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from decouple import config

from edfi_amt_data_lake.helper.data_frame_generation_result import (
    data_frame_generation_result,
)
from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    create_parquet_file,
    get_descriptor_code_value_from_uri,
    is_data_frame_empty,
    jsonNormalize,
    pdMerge,
    renameColumns,
)

ENDPOINT_ASSESSSMENTS = 'assessments'
ENDPOINT_OBJECTIVEASSESSMENTS = 'objectiveAssessments'
ENDPOINT_ASSESSMENT_CATEGORY_DESCRIPTOR = 'assessmentCategoryDescriptors'
ENDPOINT_GRADE_LEVEL_DESCRIPTOR = 'gradeLevelDescriptors'
ENDPOINT_ASSESSMENT_REPORTING_METHOD_DESCRIPTOR = 'assessmentReportingMethodDescriptors'
ENDPOINT_ACADEMIC_SUBJECT_DESCRIPTOR = 'academicSubjectDescriptors'
ENDPOINT_RESULT_DATATYPE_TYPE_DESCRIPTOR = 'resultDatatypeTypeDescriptors'

RESULT_COLUMNS = [
    'AssessmentFactKey',
    'AssessmentKey',
    'AssessmentIdentifier',
    'Namespace',
    'Title',
    'Version',
    'Category',
    'AssessedGradeLevel',
    'AcademicSubject',
    'ResultDataType',
    'ReportingMethod',
    'ObjectiveAssessmentKey',
    'IdentificationCode',
    'ParentObjectiveAssessmentKey',
    'ObjectiveAssessmentDescription',
    'PercentOfAssessment',
    'MinScore',
    'MaxScore',
    'LearningStandard'
]


@create_parquet_file
def assessment_fact_data_frame(
    file_name: str,
    columns: list[str],
    school_year: int
):
    file_name = file_name
    silverDataLocation = config('SILVER_DATA_LOCATION')
    assessmentsContent = getEndpointJson(ENDPOINT_ASSESSSMENTS, silverDataLocation, school_year)
    assessment_category_descriptor_content = getEndpointJson(ENDPOINT_ASSESSMENT_CATEGORY_DESCRIPTOR, silverDataLocation, school_year)
    objectiveAssessmentsContent = getEndpointJson(ENDPOINT_OBJECTIVEASSESSMENTS, silverDataLocation, school_year)
    grade_level_descriptor_content = getEndpointJson(ENDPOINT_GRADE_LEVEL_DESCRIPTOR, silverDataLocation, school_year)
    assessment_reporting_method_descriptor_content = getEndpointJson(ENDPOINT_ASSESSMENT_REPORTING_METHOD_DESCRIPTOR, silverDataLocation, school_year)
    academic_subject_descriptor_content = getEndpointJson(ENDPOINT_ACADEMIC_SUBJECT_DESCRIPTOR, silverDataLocation, school_year)
    result_datatype_type_descriptor_content = getEndpointJson(ENDPOINT_RESULT_DATATYPE_TYPE_DESCRIPTOR, silverDataLocation, school_year)
    # Descriptors
    ############################
    # assessmentCategoryDescriptor
    ############################
    assessment_category_descriptor_normalized = jsonNormalize(
        assessment_category_descriptor_content,
        recordPath=None,
        meta=[
            'assessmentCategoryDescriptorId',
            'codeValue',
            'description',
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    assessment_category_descriptor_normalized = renameColumns(
        assessment_category_descriptor_normalized,
        {
            'codeValue': 'assessmentCategoryDescriptor',
            'description': 'assessmentCategoryDescriptorDescription'
        }
    )
    ############################
    # gradeLevelDescriptor
    ############################
    grade_level_descriptor_normalized = jsonNormalize(
        grade_level_descriptor_content,
        recordPath=None,
        meta=[
            'gradeLevelDescriptorId',
            'codeValue',
            'description',
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    grade_level_descriptor_normalized = renameColumns(
        grade_level_descriptor_normalized,
        {
            'codeValue': 'gradeLevelDescriptor',
            'description': 'gradeLevelDescriptorDescription'
        }
    )
    ############################
    # assessment_reporting_method_descriptor
    ############################
    assessment_reporting_method_descriptor_normalized = jsonNormalize(
        assessment_reporting_method_descriptor_content,
        recordPath=None,
        meta=[
            'assessmentReportingMethodDescriptorId',
            'codeValue',
            'description',
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    assessment_reporting_method_descriptor_normalized = renameColumns(
        assessment_reporting_method_descriptor_normalized,
        {
            'codeValue': 'assessmentReportingMethodDescriptor',
            'description': 'assessmentReportingMethodDescriptorDescription'
        }
    )
    ############################
    # academic_subject_descriptor
    ############################
    academic_subject_descriptor_normalized = jsonNormalize(
        academic_subject_descriptor_content,
        recordPath=None,
        meta=[
            'academicSubjectDescriptorId',
            'codeValue',
            'description',
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    academic_subject_descriptor_normalized = renameColumns(
        academic_subject_descriptor_normalized,
        {
            'codeValue': 'academicSubjectDescriptor',
            'description': 'academicSubjectDescriptorDescription'
        }
    )
    ############################
    # result_datatype_type_descriptor
    ############################
    result_datatype_type_descriptor_normalized = jsonNormalize(
        result_datatype_type_descriptor_content,
        recordPath=None,
        meta=[
            'resultDatatypeTypeDescriptorId',
            'codeValue',
            'description',
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    result_datatype_type_descriptor_normalized = renameColumns(
        result_datatype_type_descriptor_normalized,
        {
            'codeValue': 'resultDatatypeTypeDescriptor',
            'description': 'resultDatatypeTypeDescriptorDescription'
        }
    )
    # assessments
    assessmentsContentNormalized = jsonNormalize(
        assessmentsContent,
        recordPath=None,
        meta=[
            'assessmentIdentifier',
            'namespace',
            'assessmentCategoryDescriptor',
            'assessmentTitle',
            'assessmentVersion'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    # Assessment Assessed Grade Levels normalization
    assessmentsAssessedGradeLevelsContentNormalized = jsonNormalize(
        assessmentsContent,
        recordPath=['assessedGradeLevels'],
        meta=[
            'assessmentIdentifier',
            'namespace'
        ],
        recordMeta=['gradeLevelDescriptor'],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    get_descriptor_code_value_from_uri(assessmentsAssessedGradeLevelsContentNormalized, 'gradeLevelDescriptor')
    assessmentsAssessedGradeLevelsContentNormalized = pdMerge(
        left=assessmentsAssessedGradeLevelsContentNormalized,
        right=grade_level_descriptor_normalized,
        how='left',
        leftOn=['gradeLevelDescriptor'],
        rightOn=['gradeLevelDescriptor'],
        suffixLeft=None,
        suffixRight=None
    )
    # Assessment Scores normalization
    assessmentsScoresContentNormalized = jsonNormalize(
        assessmentsContent,
        recordPath=['scores'],
        meta=[
            'assessmentIdentifier',
            'namespace'
        ],
        recordMeta=[
            'assessmentReportingMethodDescriptor',
            'maximumScore',
            'minimumScore',
            'resultDatatypeTypeDescriptor'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    get_descriptor_code_value_from_uri(assessmentsScoresContentNormalized, 'assessmentReportingMethodDescriptor')
    get_descriptor_code_value_from_uri(assessmentsScoresContentNormalized, 'resultDatatypeTypeDescriptor')
    assessmentsScoresContentNormalized = pdMerge(
        left=assessmentsScoresContentNormalized,
        right=result_datatype_type_descriptor_normalized,
        how='left',
        leftOn=['resultDatatypeTypeDescriptor'],
        rightOn=['resultDatatypeTypeDescriptor'],
        suffixLeft=None,
        suffixRight=None
    )
    assessmentsScoresContentNormalized = pdMerge(
        left=assessmentsScoresContentNormalized,
        right=assessment_reporting_method_descriptor_normalized,
        how='left',
        leftOn=['assessmentReportingMethodDescriptor'],
        rightOn=['assessmentReportingMethodDescriptor'],
        suffixLeft=None,
        suffixRight=None
    )
    # Assessment Academic Subjects normalization
    assessmentsAcademicSubjectsContentNormalized = jsonNormalize(
        assessmentsContent,
        recordPath=['academicSubjects'],
        meta=[
            'assessmentIdentifier',
            'namespace'
        ],
        recordMeta=['academicSubjectDescriptor'],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    get_descriptor_code_value_from_uri(assessmentsAcademicSubjectsContentNormalized, 'academicSubjectDescriptor')
    assessmentsAcademicSubjectsContentNormalized = pdMerge(
        left=assessmentsAcademicSubjectsContentNormalized,
        right=academic_subject_descriptor_normalized,
        how='left',
        leftOn=['academicSubjectDescriptor'],
        rightOn=['academicSubjectDescriptor'],
        suffixLeft=None,
        suffixRight=None
    )
    # Assessed Grade Levels merge
    result_data_frame = pdMerge(
        left=assessmentsContentNormalized,
        right=assessmentsAssessedGradeLevelsContentNormalized,
        how='left',
        leftOn=['assessmentIdentifier', 'namespace'],
        rightOn=['assessmentIdentifier', 'namespace'],
        suffixLeft=None,
        suffixRight=None
    )
    if is_data_frame_empty(result_data_frame):
        return None
    # Scores merge
    result_data_frame = pdMerge(
        left=result_data_frame,
        right=assessmentsScoresContentNormalized,
        how='left',
        leftOn=['assessmentIdentifier', 'namespace'],
        rightOn=['assessmentIdentifier', 'namespace'],
        suffixLeft=None,
        suffixRight=None
    )
    if is_data_frame_empty(result_data_frame):
        return None
    # Academic Subjects merge
    result_data_frame = pdMerge(
        left=result_data_frame,
        right=assessmentsAcademicSubjectsContentNormalized,
        how='left',
        leftOn=['assessmentIdentifier', 'namespace'],
        rightOn=['assessmentIdentifier', 'namespace'],
        suffixLeft=None,
        suffixRight=None
    )
    if is_data_frame_empty(result_data_frame):
        return None
    # Objective Assessment
    objectiveAssessmentsContentNormalized = jsonNormalize(
        objectiveAssessmentsContent,
        recordPath=None,
        meta=[
            'assessmentReference.namespace',
            'assessmentReference.assessmentIdentifier',
            'identificationCode',
            'parentObjectiveAssessmentReference.assessmentIdentifier',
            'parentObjectiveAssessmentReference.identificationCode',
            'parentObjectiveAssessmentReference.namespace',
            'description',
            'percentOfAssessment'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    # Objective Assessment Scores normalization
    objectiveAssessmentsScoresContentNormalized = jsonNormalize(
        objectiveAssessmentsContent,
        recordPath=['scores'],
        meta=[
            ['assessmentReference', 'assessmentIdentifier'],
            ['assessmentReference', 'namespace'],
            'identificationCode'
        ],
        recordMeta=[
            'assessmentReportingMethodDescriptor',
            'maximumScore',
            'minimumScore',
            'resultDatatypeTypeDescriptor'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    get_descriptor_code_value_from_uri(objectiveAssessmentsScoresContentNormalized, 'assessmentReportingMethodDescriptor')
    get_descriptor_code_value_from_uri(objectiveAssessmentsScoresContentNormalized, 'resultDatatypeTypeDescriptor')
    objectiveAssessmentsScoresContentNormalized = pdMerge(
        left=objectiveAssessmentsScoresContentNormalized,
        right=result_datatype_type_descriptor_normalized,
        how='left',
        leftOn=['resultDatatypeTypeDescriptor'],
        rightOn=['resultDatatypeTypeDescriptor'],
        suffixLeft=None,
        suffixRight=None
    )
    objectiveAssessmentsScoresContentNormalized = pdMerge(
        left=objectiveAssessmentsScoresContentNormalized,
        right=assessment_reporting_method_descriptor_normalized,
        how='left',
        leftOn=['assessmentReportingMethodDescriptor'],
        rightOn=['assessmentReportingMethodDescriptor'],
        suffixLeft=None,
        suffixRight=None
    )
    # Objective Assessment Learning Standards normalization
    objectiveAssessmentsLearningStandardsContentNormalized = jsonNormalize(
        objectiveAssessmentsContent,
        recordPath=['learningStandards'],
        meta=[
            ['assessmentReference', 'assessmentIdentifier'],
            ['assessmentReference', 'namespace'],
            'identificationCode'
        ],
        recordMeta=[
            'learningStandardReference.learningStandardId',
            'learningStandardReference.link.href'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    # Objective Scores merge
    result_objective_data_frame = pdMerge(
        left=objectiveAssessmentsContentNormalized,
        right=objectiveAssessmentsScoresContentNormalized,
        how='left',
        leftOn=['assessmentReference.assessmentIdentifier', 'assessmentReference.namespace', 'identificationCode'],
        rightOn=['assessmentReference.assessmentIdentifier', 'assessmentReference.namespace', 'identificationCode'],
        suffixLeft=None,
        suffixRight=None
    )
    if is_data_frame_empty(result_objective_data_frame):
        return None
    # Objective Learning Standards merge
    result_objective_data_frame = pdMerge(
        left=result_objective_data_frame,
        right=objectiveAssessmentsLearningStandardsContentNormalized,
        how='left',
        leftOn=['assessmentReference.assessmentIdentifier', 'assessmentReference.namespace', 'identificationCode'],
        rightOn=['assessmentReference.assessmentIdentifier', 'assessmentReference.namespace', 'identificationCode'],
        suffixLeft=None,
        suffixRight=None
    )
    if is_data_frame_empty(result_objective_data_frame) is None:
        return None
    # Merge Assessment data and Objective Assessment data
    result_data_frame = pdMerge(
        left=result_data_frame,
        right=result_objective_data_frame,
        how='left',
        leftOn=['assessmentIdentifier', 'namespace'],
        rightOn=['assessmentReference.assessmentIdentifier', 'assessmentReference.namespace'],
        suffixLeft=None,
        suffixRight='_objective'
    )
    if is_data_frame_empty(result_data_frame):
        return None
    # Removes namespace from Category Descriptor
    get_descriptor_code_value_from_uri(result_data_frame, 'assessmentCategoryDescriptor')
    result_data_frame = pdMerge(
        left=result_data_frame,
        right=assessment_category_descriptor_normalized,
        how='left',
        leftOn=['assessmentCategoryDescriptor'],
        rightOn=['assessmentCategoryDescriptor'],
        suffixLeft=None,
        suffixRight=None
    )
    # Removes namespace from Assessed Grade Level Descriptor
    get_descriptor_code_value_from_uri(result_data_frame, 'gradeLevelDescriptor')

    # Removes namespace from Academic Subject Descriptor
    get_descriptor_code_value_from_uri(result_data_frame, 'academicSubjectDescriptor')

    # Removes namespace from Result Data Type Descriptor
    get_descriptor_code_value_from_uri(result_data_frame, 'resultDatatypeTypeDescriptor')

    # Removes namespace from Reporting Method Descriptor
    get_descriptor_code_value_from_uri(result_data_frame, 'assessmentReportingMethodDescriptor')

    # Removes namespace from Objective Assessment Reporting Method Descriptor
    get_descriptor_code_value_from_uri(result_data_frame, 'assessmentReportingMethodDescriptor_objective')

    # Removes namespace from Objective Result Datatype Type Descriptor
    get_descriptor_code_value_from_uri(result_data_frame, 'resultDatatypeTypeDescriptor_objective')

    # Replace any N/A value with empty
    result_data_frame = result_data_frame.fillna('')

    # Concatanation fields
    result_data_frame['AssessmentFactKey'] = (
        result_data_frame['assessmentIdentifier'] + '-'
        + result_data_frame['namespace'] + '-'
        + result_data_frame['gradeLevelDescriptorId'].astype(str) + '-'
        + result_data_frame['assessmentReportingMethodDescriptorId'].astype(str) + '-'
        + result_data_frame['academicSubjectDescriptorId'].astype(str) + '-'
        + result_data_frame['identificationCode'] + '-'
        + result_data_frame['parentObjectiveAssessmentReference.identificationCode'] + '-'
        + result_data_frame['assessmentReportingMethodDescriptorId_objective'].astype(str) + '-'
        + result_data_frame['learningStandardReference.learningStandardId']
    )

    result_data_frame['AssessmentKey'] = (
        result_data_frame['assessmentIdentifier'] + '-'
        + result_data_frame['namespace']
    )

    result_data_frame['ObjectiveAssessmentKey'] = (
        result_data_frame['assessmentReference.assessmentIdentifier'] + '-'
        + result_data_frame['identificationCode'] + '-'
        + result_data_frame['assessmentReference.namespace']
    )

    result_data_frame['ParentObjectiveAssessmentKey'] = (
        result_data_frame['parentObjectiveAssessmentReference.assessmentIdentifier'] + '-'
        + result_data_frame['parentObjectiveAssessmentReference.identificationCode'] + '-'
        + result_data_frame['parentObjectiveAssessmentReference.namespace']
    )

    # If this field has '--' it's because there is no Key
    result_data_frame.loc[result_data_frame.ObjectiveAssessmentKey == '--', 'ObjectiveAssessmentKey'] = ''
    result_data_frame.loc[result_data_frame.ParentObjectiveAssessmentKey == '--', 'ParentObjectiveAssessmentKey'] = ''

    # Rename columns to match AMT
    result_data_frame = renameColumns(result_data_frame, {
        'assessmentIdentifier': 'AssessmentIdentifier',
        'namespace': 'Namespace',
        'assessmentTitle': 'Title',
        'assessmentVersion': 'Version',
        'assessmentCategoryDescriptorDescription': 'Category',
        'gradeLevelDescriptorDescription': 'AssessedGradeLevel',
        'academicSubjectDescriptorDescription': 'AcademicSubject',
        'resultDatatypeTypeDescriptorDescription': 'ResultDataType',
        'assessmentReportingMethodDescriptorDescription': 'ReportingMethod',
        'identificationCode': 'IdentificationCode',
        'description': 'ObjectiveAssessmentDescription',
        'minimumScore': 'MinScore',
        'maximumScore': 'MaxScore',
        'percentOfAssessment': 'PercentOfAssessment',
        'learningStandardReference.learningStandardId': 'LearningStandard'
    })

    result_data_frame.loc[result_data_frame['ResultDataType'] == '', 'ResultDataType'] = result_data_frame['resultDatatypeTypeDescriptorDescription_objective']
    result_data_frame.loc[result_data_frame['ReportingMethod'] == '', 'ReportingMethod'] = result_data_frame['assessmentReportingMethodDescriptorDescription_objective']

    result_data_frame.loc[result_data_frame['MinScore'] == '', 'MinScore'] = result_data_frame['minimumScore_objective']
    result_data_frame.loc[result_data_frame['MaxScore'] == '', 'MaxScore'] = result_data_frame['maximumScore_objective']

    # Converting some fields to str as preparation for the parquet file.
    result_data_frame['Version'] = result_data_frame['Version'].astype(str)
    result_data_frame['PercentOfAssessment'] = result_data_frame['PercentOfAssessment'].astype(str)

    # Reorder columns to match AMT
    result_data_frame = result_data_frame[
        columns
    ]
    return result_data_frame


def assessment_fact(school_year) -> data_frame_generation_result:
    return assessment_fact_data_frame(
        file_name="asmt_AssessmentFact.parquet",
        columns=RESULT_COLUMNS,
        school_year=school_year
    )

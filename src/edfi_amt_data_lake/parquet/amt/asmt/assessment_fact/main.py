# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from decouple import config

from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    addColumnIfNotExists,
    get_descriptor_code_value_from_uri,
    jsonNormalize,
    pdMerge,
    renameColumns,
    saveParquetFile,
    subset,
)

ENDPOINT_ASSESSSMENTS = 'assessments'
ENDPOINT_OBJECTIVEASSESSMENTS = 'objectiveAssessments'


def assessment_fact(school_year) -> None:

    silverDataLocation = config('SILVER_DATA_LOCATION')
    assessmentsContent = getEndpointJson(ENDPOINT_ASSESSSMENTS, silverDataLocation, school_year)
    objectiveAssessmentsContent = getEndpointJson(ENDPOINT_OBJECTIVEASSESSMENTS, silverDataLocation, school_year)

    assessmentsContentNormalized = jsonNormalize(
        assessmentsContent,
        recordPath=None,
        meta=None,
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    # Keep the fields I actually need
    assessmentsContentNormalized = subset(assessmentsContentNormalized, [
        'assessmentIdentifier',
        'namespace',
        'assessmentCategoryDescriptor',
        'assessmentTitle',
        'assessmentVersion'
    ])

    # Assessment Assessed Grade Levels normalization
    assessmentsAssessedGradeLevelsContentNormalized = jsonNormalize(
        assessmentsContent,
        recordPath=['assessedGradeLevels'],
        meta=['assessmentIdentifier', 'namespace'],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    # Assessment Scores normalization
    assessmentsScoresContentNormalized = jsonNormalize(
        assessmentsContent,
        recordPath=['scores'],
        meta=['assessmentIdentifier', 'namespace'],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    # Assessment Academic Subjects normalization
    assessmentsAcademicSubjectsContentNormalized = jsonNormalize(
        assessmentsContent,
        recordPath=['academicSubjects'],
        meta=['assessmentIdentifier', 'namespace'],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    # Assessed Grade Levels merge
    restultDataFrame = pdMerge(
        left=assessmentsContentNormalized,
        right=assessmentsAssessedGradeLevelsContentNormalized,
        how='left',
        leftOn=['assessmentIdentifier', 'namespace'],
        rightOn=['assessmentIdentifier', 'namespace'],
        suffixLeft=None,
        suffixRight=None
    )

    # Scores merge
    restultDataFrame = pdMerge(
        left=restultDataFrame,
        right=assessmentsScoresContentNormalized,
        how='left',
        leftOn=['assessmentIdentifier', 'namespace'],
        rightOn=['assessmentIdentifier', 'namespace'],
        suffixLeft=None,
        suffixRight=None
    )

    # Academic Subjects merge
    restultDataFrame = pdMerge(
        left=restultDataFrame,
        right=assessmentsAcademicSubjectsContentNormalized,
        how='left',
        leftOn=['assessmentIdentifier', 'namespace'],
        rightOn=['assessmentIdentifier', 'namespace'],
        suffixLeft=None,
        suffixRight=None
    )

    # Objective Assessment
    objectiveAssessmentsContentNormalized = jsonNormalize(
        objectiveAssessmentsContent,
        recordPath=None,
        meta=None,
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    addColumnIfNotExists(objectiveAssessmentsContentNormalized, 'parentObjectiveAssessmentReference.assessmentIdentifier')
    addColumnIfNotExists(objectiveAssessmentsContentNormalized, 'parentObjectiveAssessmentReference.identificationCode')
    addColumnIfNotExists(objectiveAssessmentsContentNormalized, 'parentObjectiveAssessmentReference.namespace')
    addColumnIfNotExists(objectiveAssessmentsContentNormalized, 'description')

    # Keep the fields I actually need
    objectiveAssessmentsContentNormalized = subset(objectiveAssessmentsContentNormalized, [
        'assessmentReference.namespace',
        'assessmentReference.assessmentIdentifier',
        'identificationCode',
        'parentObjectiveAssessmentReference.assessmentIdentifier',
        'parentObjectiveAssessmentReference.identificationCode',
        'parentObjectiveAssessmentReference.namespace',
        'description',
        'percentOfAssessment'
    ])

    # Objective Assessment Scores normalization
    objectiveAssessmentsScoresContentNormalized = jsonNormalize(
        objectiveAssessmentsContent,
        recordPath=['scores'],
        meta=[
            ['assessmentReference', 'assessmentIdentifier'],
            ['assessmentReference', 'namespace'],
            'identificationCode'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
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
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    # Objective Scores merge
    restultObjectiveDataFrame = pdMerge(
        left=objectiveAssessmentsContentNormalized,
        right=objectiveAssessmentsScoresContentNormalized,
        how='left',
        leftOn=['assessmentReference.assessmentIdentifier', 'assessmentReference.namespace', 'identificationCode'],
        rightOn=['assessmentReference.assessmentIdentifier', 'assessmentReference.namespace', 'identificationCode'],
        suffixLeft=None,
        suffixRight=None
    )

    # Objective Learning Standards merge
    restultObjectiveDataFrame = pdMerge(
        left=restultObjectiveDataFrame,
        right=objectiveAssessmentsLearningStandardsContentNormalized,
        how='left',
        leftOn=['assessmentReference.assessmentIdentifier', 'assessmentReference.namespace', 'identificationCode'],
        rightOn=['assessmentReference.assessmentIdentifier', 'assessmentReference.namespace', 'identificationCode'],
        suffixLeft=None,
        suffixRight=None
    )

    # Merge Assessment data and Objective Assessment data
    restultDataFrame = pdMerge(
        left=restultDataFrame,
        right=restultObjectiveDataFrame,
        how='left',
        leftOn=['assessmentIdentifier', 'namespace'],
        rightOn=['assessmentReference.assessmentIdentifier', 'assessmentReference.namespace'],
        suffixLeft=None,
        suffixRight='_objective'
    )

    # Removes namespace from Category Descriptor
    get_descriptor_code_value_from_uri(restultDataFrame, 'assessmentCategoryDescriptor')

    # Removes namespace from Assessed Grade Level Descriptor
    get_descriptor_code_value_from_uri(restultDataFrame, 'gradeLevelDescriptor')

    # Removes namespace from Academic Subject Descriptor
    get_descriptor_code_value_from_uri(restultDataFrame, 'academicSubjectDescriptor')

    # Removes namespace from Result Data Type Descriptor
    get_descriptor_code_value_from_uri(restultDataFrame, 'resultDatatypeTypeDescriptor')

    # Removes namespace from Reporting Method Descriptor
    get_descriptor_code_value_from_uri(restultDataFrame, 'assessmentReportingMethodDescriptor')

    # Removes namespace from Objective Assessment Reporting Method Descriptor
    get_descriptor_code_value_from_uri(restultDataFrame, 'assessmentReportingMethodDescriptor_objective')

    # Removes namespace from Objective Result Datatype Type Descriptor
    get_descriptor_code_value_from_uri(restultDataFrame, 'resultDatatypeTypeDescriptor_objective')

    # Replace any N/A value with empty
    restultDataFrame = restultDataFrame.fillna('')

    # Concatanation fields
    restultDataFrame['AssessmentFactKey'] = (
        restultDataFrame['assessmentIdentifier'] + '-'
        + restultDataFrame['namespace'] + '-'
        + restultDataFrame['gradeLevelDescriptor'] + '-'
        + restultDataFrame['assessmentReportingMethodDescriptor'] + '-'
        + restultDataFrame['academicSubjectDescriptor'] + '-'
        + restultDataFrame['identificationCode'] + '-'
        + restultDataFrame['parentObjectiveAssessmentReference.identificationCode'] + '-'
        + restultDataFrame['resultDatatypeTypeDescriptor_objective'] + '-'
        + restultDataFrame['learningStandardReference.learningStandardId']
    )

    restultDataFrame['AssessmentKey'] = (
        restultDataFrame['assessmentIdentifier'] + '-'
        + restultDataFrame['namespace']
    )

    restultDataFrame['ObjectiveAssessmentKey'] = (
        restultDataFrame['assessmentReference.assessmentIdentifier'] + '-'
        + restultDataFrame['identificationCode'] + '-'
        + restultDataFrame['assessmentReference.namespace']
    )

    restultDataFrame['ParentObjectiveAssessmentKey'] = (
        restultDataFrame['parentObjectiveAssessmentReference.assessmentIdentifier'] + '-'
        + restultDataFrame['parentObjectiveAssessmentReference.identificationCode'] + '-'
        + restultDataFrame['parentObjectiveAssessmentReference.namespace']
    )

    # If this field has '--' it's because there is no Key
    restultDataFrame.loc[restultDataFrame.ObjectiveAssessmentKey == '--', 'ObjectiveAssessmentKey'] = ''
    restultDataFrame.loc[restultDataFrame.ParentObjectiveAssessmentKey == '--', 'ParentObjectiveAssessmentKey'] = ''

    # Rename columns to match AMT
    restultDataFrame = renameColumns(restultDataFrame, {
        'assessmentIdentifier': 'AssessmentIdentifier',
        'namespace': 'Namespace',
        'assessmentTitle': 'Title',
        'assessmentVersion': 'Version',
        'assessmentCategoryDescriptor': 'Category',
        'gradeLevelDescriptor': 'AssessedGradeLevel',
        'academicSubjectDescriptor': 'AcademicSubject',
        'resultDatatypeTypeDescriptor': 'ResultDataType',
        'assessmentReportingMethodDescriptor': 'ReportingMethod',
        'identificationCode': 'IdentificationCode',
        'description': 'ObjectiveAssessmentDescription',
        'minimumScore': 'MinScore',
        'maximumScore': 'MaxScore',
        'percentOfAssessment': 'PercentOfAssessment',
        'learningStandardReference.learningStandardId': 'LearningStandard'
    })

    restultDataFrame.loc[restultDataFrame['ResultDataType'] == '', 'ResultDataType'] = restultDataFrame['resultDatatypeTypeDescriptor_objective']
    restultDataFrame.loc[restultDataFrame['ReportingMethod'] == '', 'ReportingMethod'] = restultDataFrame['assessmentReportingMethodDescriptor_objective']

    restultDataFrame.loc[restultDataFrame['MinScore'] == '', 'MinScore'] = restultDataFrame['minimumScore_objective']
    restultDataFrame.loc[restultDataFrame['MinScore'] == '', 'MinScore'] = restultDataFrame['maximumScore_objective']

    # Converting some fields to str as preparation for the parquet file.
    restultDataFrame['Version'] = restultDataFrame['Version'].astype(str)
    restultDataFrame['PercentOfAssessment'] = restultDataFrame['PercentOfAssessment'].astype(str)

    # Reorder columns to match AMT
    restultDataFrame = restultDataFrame[
        [
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
        ]]

    saveParquetFile(restultDataFrame, f"{config('PARQUET_FILES_LOCATION')}", "asmt_AssessmentFact.parquet", school_year)

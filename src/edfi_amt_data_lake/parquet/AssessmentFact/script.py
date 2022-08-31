# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from distutils.util import subst_vars
from operator import contains
from decouple import config
from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import jsonNormalize, pdMerge, toCsv, subset, renameColumns, saveParquetFile, addColumnIfNotExists

ENDPOINT_ASSESSSMENTS = 'assessments'
ENDPOINT_OBJECTIVEASSESSMENTS = 'objectiveAssessments'

def AssessmentFact() -> None:
    silverDataLocation = config('SILVER_DATA_LOCATION')
    assessmentsContent = getEndpointJson(ENDPOINT_ASSESSSMENTS, silverDataLocation)
    objectiveAssessmentsContent = getEndpointJson(ENDPOINT_OBJECTIVEASSESSMENTS, silverDataLocation)
    
    assessmentsContentNormalized = jsonNormalize(
        assessmentsContent,
        recordPath=None,
        meta=None,
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    # Keep the fields I actually need
    assessmentsContentNormalized = subset(assessmentsContentNormalized, 
        [
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

    # Assessment Identification Codes normalization
    assessmentsIdentificationCodesContentNormalized = jsonNormalize(
        assessmentsContent,
        recordPath=['identificationCodes'],
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

    ##  Then we need to merge the 4 data frames above:
    
    # Assessed Grade Levels merge
    restultDataFrame = pdMerge(
        left=assessmentsContentNormalized, 
        right=assessmentsAssessedGradeLevelsContentNormalized,
        how='left',
        leftOn=['assessmentIdentifier', 'namespace'],
        rigthOn=['assessmentIdentifier', 'namespace'],
        suffixLeft=None,
        suffixRight=None
    )

    # Scores merge
    restultDataFrame = pdMerge(
        left=restultDataFrame, 
        right=assessmentsScoresContentNormalized,
        how='left',
        leftOn=['assessmentIdentifier', 'namespace'],
        rigthOn=['assessmentIdentifier', 'namespace'],
        suffixLeft=None,
        suffixRight=None
    )

    # Identification Codes merge
    restultDataFrame = pdMerge(
        left=restultDataFrame, 
        right=assessmentsIdentificationCodesContentNormalized,
        how='left',
        leftOn=['assessmentIdentifier', 'namespace'],
        rigthOn=['assessmentIdentifier', 'namespace'],
        suffixLeft=None,
        suffixRight=None
    )

    # Academic Subjects merge
    restultDataFrame = pdMerge(
        left=restultDataFrame, 
        right=assessmentsAcademicSubjectsContentNormalized,
        how='left',
        leftOn=['assessmentIdentifier', 'namespace'],
        rigthOn=['assessmentIdentifier', 'namespace'],
        suffixLeft=None,
        suffixRight=None
    )

    #### Objective Assessment
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
    objectiveAssessmentsContentNormalized = subset(objectiveAssessmentsContentNormalized, 
        [
            'assessmentReference.assessmentIdentifier',
            'assessmentReference.namespace',
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
        meta=[['assessmentReference', 'assessmentIdentifier'], ['assessmentReference','namespace'], 'identificationCode'],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    
    # Objective Assessment Learning Standards normalization
    objectiveAssessmentsLearningStandardsContentNormalized = jsonNormalize(
        objectiveAssessmentsContent,
        recordPath=['learningStandards'],
        meta=[['assessmentReference', 'assessmentIdentifier'], ['assessmentReference','namespace'], 'identificationCode'],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    ##  Then we need to merge the objective data frames above:
    
    # Objective Scores merge
    restultObjectiveDataFrame = pdMerge(
        left=objectiveAssessmentsContentNormalized, 
        right=objectiveAssessmentsScoresContentNormalized,
        how='left',
        leftOn=['assessmentReference.assessmentIdentifier', 'assessmentReference.namespace', 'identificationCode'],
        rigthOn=['assessmentReference.assessmentIdentifier', 'assessmentReference.namespace', 'identificationCode'],
        suffixLeft=None,
        suffixRight=None
    )

    # Objective Learning Standards merge
    restultObjectiveDataFrame = pdMerge(
        left=restultObjectiveDataFrame, 
        right=objectiveAssessmentsLearningStandardsContentNormalized,
        how='left',
        leftOn=['assessmentReference.assessmentIdentifier', 'assessmentReference.namespace', 'identificationCode'],
        rigthOn=['assessmentReference.assessmentIdentifier', 'assessmentReference.namespace', 'identificationCode'],
        suffixLeft=None,
        suffixRight=None
    )

    # Merge Assessment data and Objective Assessment data
    restultDataFrame = pdMerge(
        left=restultDataFrame, 
        right=restultObjectiveDataFrame,
        how='left',
        leftOn=['assessmentIdentifier', 'namespace'],
        rigthOn=['assessmentReference.assessmentIdentifier', 'assessmentReference.namespace'],
        suffixLeft=None,
        suffixRight='_objective'
    )

    # Removes namespace from Category Descriptor
    if not restultDataFrame['assessmentCategoryDescriptor'].empty:
        if len(restultDataFrame['assessmentCategoryDescriptor'].str.split('#')) > 0:
            restultDataFrame["assessmentCategoryDescriptor"] = restultDataFrame["assessmentCategoryDescriptor"].str.split("#").str.get(1)

    # Removes namespace from Assessed Grade Level Descriptor
    if not restultDataFrame['gradeLevelDescriptor'].empty:
        if len(restultDataFrame['gradeLevelDescriptor'].str.split('#')) > 0:
            restultDataFrame["gradeLevelDescriptor"] = restultDataFrame["gradeLevelDescriptor"].str.split("#").str.get(1)

    # Removes namespace from Academic Subject Descriptor
    if not restultDataFrame['academicSubjectDescriptor'].empty:
        if len(restultDataFrame['academicSubjectDescriptor'].str.split('#')) > 0:
            restultDataFrame["academicSubjectDescriptor"] = restultDataFrame["academicSubjectDescriptor"].str.split("#").str.get(1)

    # Removes namespace from Result Data Type Descriptor
    if not restultDataFrame['resultDatatypeTypeDescriptor'].empty:
        if len(restultDataFrame['resultDatatypeTypeDescriptor'].str.split('#')) > 0:
            restultDataFrame["resultDatatypeTypeDescriptor"] = restultDataFrame["resultDatatypeTypeDescriptor"].str.split("#").str.get(1)

    # Removes namespace from Reporting Method Descriptor
    if not restultDataFrame['assessmentReportingMethodDescriptor'].empty:
        if len(restultDataFrame['assessmentReportingMethodDescriptor'].str.split('#')) > 0:
            restultDataFrame["assessmentReportingMethodDescriptor"] = restultDataFrame["assessmentReportingMethodDescriptor"].str.split("#").str.get(1)

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
        + restultDataFrame['assessmentReportingMethodDescriptor_objective'] + '-'
        + restultDataFrame['learningStandardReference.learningStandardId']
    )

    restultDataFrame['AssessmentKey'] = (
        restultDataFrame['assessmentIdentifier'] + '-'
        + restultDataFrame['namespace']
    )

    restultDataFrame['ObjectiveAssessmentKey'] = (
        restultDataFrame['assessmentIdentifier'] + '-'
        + restultDataFrame['identificationCode'] + '-'
        + restultDataFrame['namespace']
    )

    restultDataFrame['ParentObjectiveAssessmentKey'] = (
        restultDataFrame['parentObjectiveAssessmentReference.assessmentIdentifier'] + '-'
        + restultDataFrame['parentObjectiveAssessmentReference.identificationCode'] + '-'
        + restultDataFrame['parentObjectiveAssessmentReference.namespace']
    )

    # If this field has '--' it's because there is no Key
    restultDataFrame.loc[restultDataFrame.ParentObjectiveAssessmentKey == '--', 'ParentObjectiveAssessmentKey'] = ''

    # Rename columns to match AMT
    restultDataFrame = renameColumns(restultDataFrame, 
        {
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

    restultDataFrame.loc[restultDataFrame['ResultDataType'] == '','ResultDataType'] = restultDataFrame['resultDatatypeTypeDescriptor_objective']
    restultDataFrame.loc[restultDataFrame['ReportingMethod'] == '','ReportingMethod'] = restultDataFrame['assessmentReportingMethodDescriptor_objective']

    restultDataFrame.loc[restultDataFrame['MinScore'] == '','MinScore'] = restultDataFrame['minimumScore_objective']
    restultDataFrame.loc[restultDataFrame['MinScore'] == '','MinScore'] = restultDataFrame['maximumScore_objective']

    # Reorder columns to match AMT
    restultDataFrame = restultDataFrame[[
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
    
    saveParquetFile(restultDataFrame, f"{config('PARQUET_FILES_LOCATION')}asmt_AssessmentFact.parquet")
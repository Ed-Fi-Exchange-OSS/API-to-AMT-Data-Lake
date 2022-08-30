# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from distutils.util import subst_vars
from operator import contains
from decouple import config
from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import jsonNormalize, pdMerge, toCsv, subset, renameColumns, saveParquetFile

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

    toCsv(restultDataFrame, "C:\\temp\\edfi\\restultDataFrame.csv")

    #### 
    objectiveAssessmentsContentNormalized = jsonNormalize(
        objectiveAssessmentsContent,
        recordPath=None,
        meta=None,
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    # Keep the fields I actually need
    objectiveAssessmentsContentNormalized = subset(objectiveAssessmentsContentNormalized, 
        [
            'assessmentReference.assessmentIdentifier',
            'assessmentReference.namespace',
            'identificationCode'
        ])

    toCsv(objectiveAssessmentsContentNormalized, "C:\\temp\\edfi\\objectiveAssessmentsContentNormalized.csv")
    
    # Objective Assessment Scores normalization
    objectiveAssessmentsScoresContentNormalized = jsonNormalize(
        objectiveAssessmentsContent,
        recordPath=['scores'],
        meta=['assessmentReference.assessmentIdentifier', 'assessmentReference.namespace', 'identificationCode'],
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

    toCsv(restultObjectiveDataFrame, "C:\\temp\\edfi\\restultObjectiveDataFrame.csv")
    
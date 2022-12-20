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
    jsonNormalize,
    pdMerge,
    renameColumns,
)

ENDPOINT_CANDIDATES = 'candidates'
ENDPOINT_EVALUATION_OBJETIVES = 'evaluationObjectives'
ENDPOINT_EVALUATION_ELEMENTS_RATINGS = 'evaluationElementRatings'
ENDPOINT_TERM_DESCRIPTOR = 'termDescriptors'
RESULT_COLUMNS = [
    'candidateIdentifier',
    'evaluationObjectiveRatingReference.evaluationDate',
    'evaluationElementReference.performanceEvaluationTitle',
    'evaluationObjectiveTitle',
    'evaluationElementReference.evaluationElementTitle',
    'ratingResultTitle',
    'evaluationElementReference.evaluationTitle',
    'termDescriptorId',
    'evaluationElementReference.schoolYear',
    'rating'
]


@create_parquet_file
def evaluation_element_rating_dim_dataframe(
    file_name: str,
    columns: list[str],
    school_year: int
):
    candidates_content = getEndpointJson(ENDPOINT_CANDIDATES, config('SILVER_DATA_LOCATION'), school_year)
    evaluation_objetives_content = getEndpointJson(ENDPOINT_EVALUATION_OBJETIVES, config('SILVER_DATA_LOCATION'), school_year)
    evaluation_elements_ratings_content = getEndpointJson(ENDPOINT_EVALUATION_ELEMENTS_RATINGS, config('SILVER_DATA_LOCATION'), school_year)
    term_descriptor_content = getEndpointJson(ENDPOINT_TERM_DESCRIPTOR, config('SILVER_DATA_LOCATION'), school_year)

    evaluation_elements_ratings_normalized = jsonNormalize(
        evaluation_elements_ratings_content,
        recordPath=None,
        meta=[
            'id',
            ['evaluationObjectiveRatingReference', 'personId'],
            ['evaluationObjectiveRatingReference', 'evaluationDate'],
            ['evaluationObjectiveRatingReference', 'evaluationObjectiveTitle'],
            ['evaluationElementReference', 'performanceEvaluationTitle'],
            ['evaluationElementReference', 'evaluationElementTitle'],
            ['evaluationElementReference', 'termDescriptor'],
            ['evaluationElementReference', 'schoolYear'],
            ['evaluationElementReference', 'evaluationTitle']
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    if evaluation_elements_ratings_normalized.empty:
        return None

    evaluation_elements_ratings_results_normalized = jsonNormalize(
        evaluation_elements_ratings_content,
        recordPath=['results'],
        meta=[
            'id'
        ],
        recordMeta=[
            'ratingResultTitle',
            'rating'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    candidates_normalized = jsonNormalize(
        candidates_content,
        recordPath=None,
        meta=[
            ['personReference', 'personId'],
            'candidateIdentifier'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    if candidates_normalized.empty:
        return None

    evaluation_objetives_normalized = jsonNormalize(
        evaluation_objetives_content,
        recordPath=None,
        meta=[
            'evaluationObjectiveTitle'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    if evaluation_objetives_normalized.empty:
        return None

    term_descriptor_normalize = jsonNormalize(
        term_descriptor_content,
        recordPath=None,
        meta=[
            'termDescriptorId',
            'codeValue',
            'namespace'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    term_descriptor_normalize['namespace_with_codevalue'] = (
        term_descriptor_normalize['namespace'] + '#' + term_descriptor_normalize['codeValue']
    )

    result_data_frame = pdMerge(
        left=evaluation_elements_ratings_normalized,
        right=evaluation_elements_ratings_results_normalized,
        how='left',
        leftOn=['id'],
        rightOn=['id'],
        suffixLeft=None,
        suffixRight='_evaluation_elements_results'
    )

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=candidates_normalized,
        how='inner',
        leftOn=['evaluationObjectiveRatingReference.personId'],
        rightOn=['personReference.personId'],
        suffixLeft=None,
        suffixRight='_candidates'
    )

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=evaluation_objetives_normalized,
        how='inner',
        leftOn=['evaluationObjectiveRatingReference.evaluationObjectiveTitle'],
        rightOn=['evaluationObjectiveTitle'],
        suffixLeft=None,
        suffixRight='_evaluation_objectives'
    )

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=term_descriptor_normalize,
        how='left',
        leftOn=['evaluationElementReference.termDescriptor'],
        rightOn=['namespace_with_codevalue'],
        suffixLeft=None,
        suffixRight='_term_descriptor'
    )

    result_data_frame = result_data_frame[columns]

    result_data_frame = result_data_frame.drop_duplicates([
        'candidateIdentifier',
        'evaluationObjectiveRatingReference.evaluationDate',
        'evaluationElementReference.performanceEvaluationTitle',
        'evaluationObjectiveTitle',
        'evaluationElementReference.evaluationElementTitle',
        'ratingResultTitle',
        'evaluationElementReference.evaluationTitle',
        'termDescriptorId',
        'evaluationElementReference.schoolYear',
        'rating'
    ])
    result_data_frame['evaluationObjectiveRatingReference.evaluationDate'] = (
        result_data_frame['evaluationObjectiveRatingReference.evaluationDate'].str[:10]
    )
    result_data_frame = renameColumns(result_data_frame, {
        'candidateIdentifier': 'CandidateKey',
        'evaluationObjectiveRatingReference.evaluationDate': 'EvaluationDate',
        'evaluationElementReference.performanceEvaluationTitle': 'PerformanceEvaluationTitle',
        'evaluationObjectiveTitle': 'EvaluationObjectiveTitle',
        'evaluationElementReference.evaluationElementTitle': 'EvaluationElementTitle',
        'ratingResultTitle': 'RatingResultTitle',
        'evaluationElementReference.evaluationTitle': 'EvaluationTitle',
        'termDescriptorId': 'TermDescriptorId',
        'evaluationElementReference.schoolYear': 'SchoolYear',
        'rating': 'Rating'
    })

    return result_data_frame


def evaluation_element_rating_dim(school_year) -> data_frame_generation_result:
    return evaluation_element_rating_dim_dataframe(
        file_name="epp_EvaluationElementRatingDim.parquet",
        columns=RESULT_COLUMNS,
        school_year=school_year
    )

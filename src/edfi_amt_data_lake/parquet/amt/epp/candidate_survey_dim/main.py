# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import pandas as pd
from decouple import config

from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    addColumnIfNotExists,
    get_reference_from_href,
    jsonNormalize,
    pdMerge,
    renameColumns,
    replace_null,
    saveParquetFile,
    subset,
    to_datetime_key,
)

ENDPOINT_CANDIDATE = 'candidates'
ENDPOINT_SURVEY = 'surveys'
ENDPOINT_SURVEY_QUESTION = 'surveyQuestions'
ENDPOINT_SURVEY_RESPONSE = 'surveyResponses'
ENDPOINT_SURVEY_QUESTION_RESPONSE = 'surveyQuestionResponses'
ENDPOINT_SURVEY_RESPONSE_PERSON_TARGET_ASSOCIATION = 'surveyResponsePersonTargetAssociations'


def candidate_survey_dim_dataframe(school_year) -> pd.DataFrame:
    candidate_content = getEndpointJson(ENDPOINT_CANDIDATE, config('SILVER_DATA_LOCATION'), school_year)
    survey_content = getEndpointJson(ENDPOINT_SURVEY, config('SILVER_DATA_LOCATION'), school_year)
    survey_question_content = getEndpointJson(ENDPOINT_SURVEY_QUESTION, config('SILVER_DATA_LOCATION'), school_year)
    survey_response_content = getEndpointJson(ENDPOINT_SURVEY_RESPONSE, config('SILVER_DATA_LOCATION'), school_year)
    survey_question_response_content = getEndpointJson(ENDPOINT_SURVEY_QUESTION_RESPONSE, config('SILVER_DATA_LOCATION'), school_year)
    survey_response_person_target_association_content = getEndpointJson(ENDPOINT_SURVEY_RESPONSE_PERSON_TARGET_ASSOCIATION, config('SILVER_DATA_LOCATION'), school_year)

    ############################
    # surveys
    ############################
    survey_normalize = jsonNormalize(
        survey_content,
        recordPath=None,
        meta=[
            'id',
            'surveyIdentifier',
            'surveyTitle'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    survey_normalize = renameColumns(survey_normalize, {
        'id': 'surveyReferenceId'
    })
    # Select needed columns.
    survey_normalize = subset(survey_normalize, [
        'surveyReferenceId',
        'surveyIdentifier',
        'surveyTitle'
    ])
    ############################
    # survey question
    ############################
    survey_question_normalize = jsonNormalize(
        survey_question_content,
        recordPath=None,
        meta=[
            'id',
            'surveyReference.link.href'
            'surveySectionReference.surveyIdentifier',
            'surveySectionReference.surveySectionTitle',
            'questionCode',
            'questionText'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    get_reference_from_href(survey_question_normalize, 'id', 'surveyQuestionReferenceId')
    get_reference_from_href(survey_question_normalize, 'surveyReference.link.href', 'surveyReferenceId')
    survey_question_normalize = renameColumns(survey_question_normalize, {
        'surveySectionReference.surveyIdentifier': 'surveyIdentifier',
        'surveySectionReference.surveySectionTitle': 'surveySectionTitle',
    })
    # Select needed columns.
    survey_question_normalize = subset(survey_question_normalize, [
        'surveyReferenceId',
        'surveyQuestionReferenceId',
        'surveySectionTitle',
        'questionCode',
        'questionText'
    ])
    ############################
    # survey response
    ############################
    survey_response_normalize = jsonNormalize(
        survey_response_content,
        recordPath=None,
        meta=[
            'id',
            'studentReference.studentUniqueId',
            'studentReference.link.href',
            'surveyReference.surveyIdentifier',
            'surveyReference.link.href',
            'responseDate',
            'surveyResponseIdentifier'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    survey_response_normalize['responseDateKey'] = to_datetime_key(survey_response_normalize, 'responseDate')
    get_reference_from_href(survey_response_normalize, 'studentReference.link.href', 'studentReferenceId')
    get_reference_from_href(survey_response_normalize, 'surveyReference.link.href', 'surveyReferenceId')
    survey_response_normalize = renameColumns(survey_response_normalize, {
        'studentReference.studentUniqueId': 'studentUniqueId',
        'id': 'surveyResponseReferenceId',
    })
    # Select needed columns.
    survey_response_normalize = subset(survey_response_normalize, [
        'studentUniqueId',
        'studentReferenceId',
        'surveyReferenceId',
        'surveyResponseReferenceId',
        'responseDateKey',
        'surveyResponseIdentifier'
    ])
    ############################
    # survey question response
    ############################
    survey_question_response_values_normalize = jsonNormalize(
        survey_question_response_content,
        recordPath='surveyQuestionMatrixElementResponses',
        meta=[
            'id'
        ],
        metaPrefix=None,
        recordPrefix='values_',
        errors='ignore'
    )
    survey_question_response_values_normalize = renameColumns(survey_question_response_values_normalize, {
        'id': 'surveyQuestionResponseReferenceId',
        'values_numericResponse': 'numericResponse',
        'values_textResponse': 'textResponse',
    })
    # Select needed columns.
    survey_question_response_values_normalize = subset(survey_question_response_values_normalize, [
        'surveyQuestionResponseReferenceId',
        'numericResponse',
        'textResponse'
    ])
    ############################
    # survey question response
    ############################
    survey_question_response_normalize = jsonNormalize(
        survey_question_response_content,
        recordPath=None,
        meta=[
            'id',
            'surveyQuestionReference.link.href',
            'surveyResponseReference.link.href',
            'surveyQuestionReference.questionCode',
            'surveyQuestionReference.surveyIdentifier'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    get_reference_from_href(survey_question_response_normalize, 'surveyQuestionReference.link.href', 'surveyQuestionReferenceId')
    get_reference_from_href(survey_question_response_normalize, 'surveyResponseReference.link.href', 'surveyResponseReferenceId')
    survey_question_response_normalize = renameColumns(survey_question_response_normalize, {
        'id': 'surveyQuestionResponseReferenceId',
        'surveyQuestionReference.questionCode': 'questionCode',
        'surveyQuestionReference.surveyIdentifier': 'surveyIdentifier'
    })
    # Select needed columns.
    survey_question_response_normalize = subset(survey_question_response_normalize, [
        'surveyQuestionResponseReferenceId',
        'surveyQuestionReferenceId',
        'surveyResponseReferenceId',
        'questionCode',
        'surveyIdentifier'
    ])
    ############################
    # surveyquestionresponse
    ############################
    survey_question_response_normalize = pdMerge(
        left=survey_question_response_values_normalize,
        right=survey_question_response_normalize,
        how='inner',
        leftOn=[
            'surveyQuestionResponseReferenceId'
        ],
        rightOn=[
            'surveyQuestionResponseReferenceId'
        ],
        suffixLeft=None,
        suffixRight=None
    )
    ############################
    # survey - question - response
    ############################
    result_data_frame = pdMerge(
        left=survey_normalize,
        right=survey_question_response_normalize,
        how='inner',
        leftOn=[
            'surveyIdentifier'
        ],
        rightOn=[
            'surveyIdentifier'
        ],
        suffixLeft='_survey',
        suffixRight='_question_response'
    )
    ############################
    # survey - response
    ############################
    result_data_frame = pdMerge(
        left=result_data_frame,
        right=survey_response_normalize,
        how='inner',
        leftOn=[
            'surveyResponseReferenceId'
        ],
        rightOn=[
            'surveyResponseReferenceId'
        ],
        suffixLeft='_survey',
        suffixRight='_question_response'
    )
    ############################
    # survey - question
    ############################
    result_data_frame = pdMerge(
        left=result_data_frame,
        right=survey_question_normalize,
        how='inner',
        leftOn=[
            'surveyQuestionReferenceId',
            'questionCode'
        ],
        rightOn=[
            'surveyQuestionReferenceId',
            'questionCode'
        ],
        suffixLeft='_survey',
        suffixRight='_question_response'
    )
    ############################
    # survey_response_person_target_association
    ############################
    survey_response_person_target_association_normalize = jsonNormalize(
        survey_response_person_target_association_content,
        recordPath=None,
        meta=[
            'surveyResponseReference.link.href',
            'personReference.personId',
            'personReference.link.href',
        ],
        metaPrefix=None,
        recordPrefix="response_",
        errors='ignore'
    )
    get_reference_from_href(survey_response_person_target_association_normalize, 'surveyResponseReference.link.href', 'surveyResponseReferenceId')
    get_reference_from_href(survey_response_person_target_association_normalize, 'personReference.link.href', 'personReferenceId')
    survey_response_person_target_association_normalize = renameColumns(survey_response_person_target_association_normalize, {
        'personReference.personId': 'personId'
    })
    # Select needed columns.
    survey_response_person_target_association_normalize = subset(survey_response_person_target_association_normalize, [
        'surveyResponseReferenceId',
        'personReferenceId',
        'personId'
    ])
    ############################
    # survey - response - person
    ############################
    result_data_frame = pdMerge(
        left=result_data_frame,
        right=survey_response_person_target_association_normalize,
        how='inner',
        leftOn=['surveyResponseReferenceId'],
        rightOn=['surveyResponseReferenceId'],
        suffixLeft='_survey',
        suffixRight='_response_person'
    )
    ############################
    # candidates
    ############################
    candidate_normalize = jsonNormalize(
        candidate_content,
        recordPath=None,
        meta=[
            'candidateIdentifier',
            'personReference.link.href'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    addColumnIfNotExists(candidate_normalize, 'personReference.id', '')
    addColumnIfNotExists(candidate_normalize, 'personReference.link.href', '/')
    get_reference_from_href(candidate_normalize, 'personReference.link.href', 'personReferenceId')
    # Select needed columns.
    candidate_normalize = subset(candidate_normalize, [
        'candidateIdentifier'
        , 'personReferenceId'
    ])
    ############################
    # survey - candidate
    ############################
    result_data_frame = pdMerge(
        left=result_data_frame,
        right=candidate_normalize,
        how='inner',
        leftOn=['personReferenceId'],
        rightOn=['personReferenceId'],
        suffixLeft='_survey',
        suffixRight='_candidate'
    )
    replace_null(result_data_frame, 'candidateIdentifier', '')
    replace_null(result_data_frame, 'numericResponse', '0')
    replace_null(result_data_frame, 'textResponse', '')
    result_data_frame['surveyIdentifier'] = result_data_frame['surveyIdentifier'].astype(str)
    result_data_frame['questionCode'] = result_data_frame['questionCode'].astype(str)
    result_data_frame['surveyResponseIdentifier'] = result_data_frame['surveyResponseIdentifier'].astype(str)
    result_data_frame['personId'] = result_data_frame['personId'].astype(str)
    result_data_frame['candidateIdentifier'] = result_data_frame['candidateIdentifier'].astype(str)
    result_data_frame['candidateSurveyKey'] = (
        result_data_frame['surveyIdentifier'].astype(str)
        + '-' + result_data_frame['questionCode']
        + '-' + result_data_frame['surveyResponseIdentifier']
        + '-' + result_data_frame['personId']
    )
    result_data_frame['candidateKey'] = result_data_frame['candidateIdentifier']
    result_data_frame = renameColumns(result_data_frame, {
        'candidateSurveyKey': 'CandidateSurveyKey'
        , 'candidateKey': 'CandidateKey'
        , 'surveyTitle': 'SurveyTitle'
        , 'surveySectionTitle': 'SurveySectionTitle'
        , 'responseDateKey': 'ResponseDateKey'
        , 'questionCode': 'QuestionCode'
        , 'questionText': 'QuestionText'
        , 'numericResponse': 'NumericResponse'
        , 'textResponse': 'TextResponse'
    })
    result_data_frame = subset(result_data_frame, [
        'CandidateSurveyKey'
        , 'CandidateKey'
        , 'SurveyTitle'
        , 'SurveySectionTitle'
        , 'ResponseDateKey'
        , 'QuestionCode'
        , 'QuestionText'
        , 'NumericResponse'
        , 'TextResponse'
    ])
    return result_data_frame


def candidate_survey_dim(school_year) -> None:
    result_data_frame = candidate_survey_dim_dataframe(school_year)
    saveParquetFile(result_data_frame, f"{config('PARQUET_FILES_LOCATION')}", "epp_CandidateSurveyDim.parquet", school_year)

# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import pandas as pd
from decouple import config

from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    get_descriptor_code_value_from_uri,
    get_reference_from_href,
    jsonNormalize,
    pdMerge,
    renameColumns,
    replace_null,
    saveParquetFile,
    subset,
    to_datetime_key,
)

ENDPOINT_AID_TYPE_DESCRIPTOR = 'aidTypeDescriptors'
ENDPOINT_CANDIDATE = 'candidates'
ENDPOINT_FINANCIAL_AIDS = 'financialAids'
ENDPOINT_STUDENT = 'students'


def epp_financial_aid_fact_dataframe(school_year) -> pd.DataFrame:
    aid_type_descriptor_content = getEndpointJson(ENDPOINT_AID_TYPE_DESCRIPTOR, config('SILVER_DATA_LOCATION'), school_year)
    candidate_content = getEndpointJson(ENDPOINT_CANDIDATE, config('SILVER_DATA_LOCATION'), school_year)
    financial_aids_content = getEndpointJson(ENDPOINT_FINANCIAL_AIDS, config('SILVER_DATA_LOCATION'), school_year)
    student_content = getEndpointJson(ENDPOINT_STUDENT, config('SILVER_DATA_LOCATION'), school_year)

    ############################
    # financialAids
    ############################
    financial_aids_normalize = jsonNormalize(
        financial_aids_content,
        recordPath=None,
        meta=[
            'beginDate',
            'endDate',
            'aidConditionDescription',
            'aidTypeDescriptor',
            'aidAmount',
            'pellGrantRecipient',
            'studentReference.studentUniqueId',
            'studentReference.link.href'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    get_reference_from_href(
        financial_aids_normalize,
        'studentReference.link.href',
        'studentReferenceId'
    )
    replace_null(
        financial_aids_normalize,
        'endDate',
        ''
    )
    financial_aids_normalize.loc[financial_aids_normalize['pellGrantRecipient'].astype(str) == 'True', 'pellGrantRecipient'] = '1'
    financial_aids_normalize.loc[financial_aids_normalize['pellGrantRecipient'].astype(str) == 'False', 'pellGrantRecipient'] = '0'
    get_descriptor_code_value_from_uri(financial_aids_normalize, 'aidTypeDescriptor')
    financial_aids_normalize['beginDateKey'] = to_datetime_key(financial_aids_normalize, 'beginDate')
    financial_aids_normalize['beginDateKey'] = financial_aids_normalize['beginDateKey'].astype(str)
    financial_aids_normalize = renameColumns(financial_aids_normalize, {
        'aidTypeDescriptor': 'aidTypeDescriptorCodeValue'
    })
    # Select needed columns.
    financial_aids_normalize = subset(financial_aids_normalize, [
        'beginDateKey',
        'beginDate',
        'endDate',
        'aidConditionDescription',
        'aidTypeDescriptorCodeValue',
        'aidAmount',
        'pellGrantRecipient',
        'studentReference.studentUniqueId',
        'studentReferenceId'
    ])
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
    replace_null(candidate_normalize, 'personReference.link.href', '/')
    get_reference_from_href(candidate_normalize, 'personReference.link.href', 'personReferenceId')
    # Select needed columns.
    candidate_normalize = subset(candidate_normalize, [
        'candidateIdentifier'
        , 'personReferenceId'
    ])
    candidate_normalize = candidate_normalize[candidate_normalize['personReferenceId'] != '']
    ############################
    # students
    ############################
    student_normalize = jsonNormalize(
        student_content,
        recordPath=None,
        meta=[
            'id',
            'personReference.link.href'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    replace_null(student_normalize, 'personReference.link.href', '/')
    get_reference_from_href(student_normalize, 'personReference.link.href', 'personReferenceId')
    student_normalize = student_normalize[student_normalize['personReferenceId'] != '']
    student_normalize = renameColumns(student_normalize, {
        'id': 'studentReferenceId'
    })
    # Select needed columns.
    student_normalize = subset(student_normalize, [
        'studentReferenceId',
        'personReferenceId'
    ])
    ############################
    # aidDescriptor
    ############################
    aid_type_descriptor_normalize = jsonNormalize(
        aid_type_descriptor_content,
        recordPath=None,
        meta=[
            'aidTypeDescriptorId',
            'codeValue',
            'description'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    replace_null(aid_type_descriptor_normalize, 'personReference.link.href', '/')
    get_reference_from_href(aid_type_descriptor_normalize, 'personReference.link.href', 'personReferenceId')
    aid_type_descriptor_normalize = renameColumns(aid_type_descriptor_normalize, {
        'codeValue': 'aidTypeDescriptorCodeValue',
        'description': 'aidTypeDescriptordescription'
    })
    aid_type_descriptor_normalize['aidTypeDescriptorId'] = aid_type_descriptor_normalize['aidTypeDescriptorId'].astype(str)
    # Select needed columns.
    aid_type_descriptor_normalize = subset(aid_type_descriptor_normalize, [
        'aidTypeDescriptorId',
        'aidTypeDescriptorCodeValue'
    ])
    ############################
    # student - candidate
    ############################
    result_data_frame = pdMerge(
        left=candidate_normalize,
        right=student_normalize,
        how='inner',
        leftOn=[
            'personReferenceId'
        ],
        rightOn=[
            'personReferenceId'
        ],
        suffixLeft=None,
        suffixRight=None
    )
    ############################
    # financialAids - Student
    ############################
    result_data_frame = pdMerge(
        left=result_data_frame,
        right=financial_aids_normalize,
        how='left',
        leftOn=[
            'studentReferenceId'
        ],
        rightOn=[
            'studentReferenceId'
        ],
        suffixLeft=None,
        suffixRight=None
    )
    ############################
    # financialAids - descriptor
    ############################
    result_data_frame = pdMerge(
        left=result_data_frame,
        right=aid_type_descriptor_normalize,
        how='left',
        leftOn=[
            'aidTypeDescriptorCodeValue'
        ],
        rightOn=[
            'aidTypeDescriptorCodeValue'
        ],
        suffixLeft=None,
        suffixRight=None
    )
    result_data_frame.fillna('')
    replace_null(result_data_frame, 'candidateIdentifier', '')
    replace_null(result_data_frame, 'aidTypeDescriptorId', '')
    replace_null(result_data_frame, 'beginDateKey', '')
    replace_null(result_data_frame, 'aidConditionDescription', '')
    replace_null(result_data_frame, 'aidAmount', '0')
    replace_null(result_data_frame, 'pellGrantRecipient', '0')
    result_data_frame['aidTypeDescriptorId'] = result_data_frame['aidTypeDescriptorId'].astype(str)
    result_data_frame['candidateAidKey'] = (
        result_data_frame['candidateIdentifier']
        + '-' + result_data_frame['aidTypeDescriptorId']
        + '-' + result_data_frame['beginDateKey']
    )
    result_data_frame['candidateKey'] = result_data_frame['candidateIdentifier'].astype(str)
    result_data_frame['aidType'] = result_data_frame['aidTypeDescriptorCodeValue'].astype(str)
    result_data_frame = renameColumns(result_data_frame, {
        'candidateAidKey': 'CandidateAidKey'
        , 'candidateKey': 'CandidateKey'
        , 'beginDate': 'BeginDate'
        , 'endDate': 'EndDate'
        , 'aidConditionDescription': 'AidConditionDescription'
        , 'aidType': 'AidType'
        , 'aidAmount': 'AidAmount'
        , 'pellGrantRecipient': 'PellGrantRecipient'
    })
    result_data_frame = subset(result_data_frame, [
        'CandidateAidKey'
        , 'CandidateKey'
        , 'BeginDate'
        , 'EndDate'
        , 'AidConditionDescription'
        , 'AidType'
        , 'AidAmount'
        , 'PellGrantRecipient'
    ])
    return result_data_frame


def epp_financial_aid_fact(school_year) -> None:
    result_data_frame = epp_financial_aid_fact_dataframe(school_year)
    saveParquetFile(result_data_frame, f"{config('PARQUET_FILES_LOCATION')}", "epp_FinancialAidFact.parquet", school_year)

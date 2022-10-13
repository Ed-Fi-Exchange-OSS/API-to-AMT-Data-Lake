# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from datetime import date

from decouple import config

from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    jsonNormalize,
    pdMerge,
    renameColumns,
    saveParquetFile,
    subset,
    to_datetime_key,
    toCsv,
)


ENDPOINT_CANDIDATES = 'candidates'
ENDPOINT_STUDENTS = 'students'
ENDPOINT_PEOPLE = 'people'
ENDPOINT_CREDENTIALS = 'credentials'
ENDPOINT_CANDIDATE_EDUCATOR_PREPARATION_PROGRAM_ASSOCIATIONS = 'candidateEducatorPreparationProgramAssociations'

def candidate_dim(school_year) -> None:
    candidates_content = getEndpointJson(
        ENDPOINT_CANDIDATES, config('SILVER_DATA_LOCATION'), school_year
    )
    students_content = getEndpointJson(
        ENDPOINT_STUDENTS, config('SILVER_DATA_LOCATION'), school_year
    )
    people_content = getEndpointJson(
        ENDPOINT_PEOPLE, config('SILVER_DATA_LOCATION'), school_year
    )
    credentials_content = getEndpointJson(
        ENDPOINT_CREDENTIALS, config('SILVER_DATA_LOCATION'), school_year
    )
    candidate_educator_preparation_program_associations_content = getEndpointJson(
        ENDPOINT_CANDIDATE_EDUCATOR_PREPARATION_PROGRAM_ASSOCIATIONS, config('SILVER_DATA_LOCATION'), school_year
    )

    candidates_normalized = jsonNormalize(
        candidates_content,
        recordPath=None,
        meta=[
            'firstName',
            'lastSurname',
            'sexDescriptor',
            'hispanicLatinoEthnicity',
            'economicDisadvantaged'
        ],
        metaPrefix=None,
        recordPrefix='candidates_',
        errors='ignore'
    )
    # toCsv(candidates_normalized, "C:/temp/edfi/parquet", "candidates_normalized.csv", "")

    students_normalized = jsonNormalize(
        students_content,
        recordPath=None,
        meta=[
            'studentUniqueId',
            'id',
            'personReference.personId'
        ],
        metaPrefix=None,
        recordPrefix='students_',
        errors='ignore'
    )
    # toCsv(students_normalized, "C:/temp/edfi/parquet", "students_normalized.csv", "")

    people_normalized = jsonNormalize(
        people_content,
        recordPath=None,
        meta=[
            'personId',
            'id'
        ],
        metaPrefix=None,
        recordPrefix='people_',
        errors='ignore'
    )
    # toCsv(people_normalized, "C:/temp/edfi/parquet", "people_normalized.csv", "")

    credentials_normalized = jsonNormalize(
        credentials_content,
        recordPath=None,
        meta=[
            'credentialIdentifier',
            'issuanceDate'
        ],
        metaPrefix=None,
        recordPrefix='credentials_',
        errors='ignore'
    )
    # toCsv(credentials_normalized, "C:/temp/edfi/parquet", "credentials_normalized.csv", "")

    candidate_educator_preparation_program_associations_normalized = jsonNormalize(
        candidate_educator_preparation_program_associations_content,
        recordPath=[
            'cohortYears'
        ],
        meta=['beginDate',
            ['candidateReference','candidateIdentifier'],
            ['educatorPreparationProgramReference','programName'],
            ['educatorPreparationProgramReference','educationOrganizationId']],
        metaPrefix=None,
        recordPrefix='candidate_educator_preparation_program_',
        errors='ignore'
    )
    # toCsv(candidate_educator_preparation_program_associations_normalized, "C:/temp/edfi/parquet", "candidate_educator_preparation_program_associations_normalized.csv", "")

    result_data_frame = pdMerge(
        left=candidates_normalized,
        right=candidate_educator_preparation_program_associations_normalized,
        how='left',
        leftOn=['candidateIdentifier'],
        rigthOn=['candidateReference.candidateIdentifier'],
        suffixLeft=None,
        suffixRight='_person'
    )

    saveParquetFile(result_data_frame, f"{config('PARQUET_FILES_LOCATION')}", "epp_CandidateDim.parquet", school_year)

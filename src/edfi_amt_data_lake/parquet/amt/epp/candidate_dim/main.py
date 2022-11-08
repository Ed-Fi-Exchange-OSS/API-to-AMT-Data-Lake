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
    replace_null,
    saveParquetFile,
)

ENDPOINT_CANDIDATES = 'candidates'
ENDPOINT_STUDENTS = 'students'
ENDPOINT_PEOPLE = 'people'
ENDPOINT_CREDENTIALS = 'credentials'
ENDPOINT_CANDIDATE_EDUCATOR_PREPARATION_PROGRAM_ASSOCIATIONS = 'candidateEducatorPreparationProgramAssociations'

ENDPOINT_RACES_DESCRIPTOR = 'raceDescriptors'
ENDPOINT_SEX_DESCRIPTOR = 'sexDescriptors'


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
    races_descriptor_content = getEndpointJson(
        ENDPOINT_RACES_DESCRIPTOR, config('SILVER_DATA_LOCATION'), school_year
    )
    sex_descriptor_content = getEndpointJson(
        ENDPOINT_SEX_DESCRIPTOR, config('SILVER_DATA_LOCATION'), school_year
    )

    races_descriptor_normalized = jsonNormalize(
        races_descriptor_content,
        recordPath=None,
        meta=[
            'raceDescriptorId',
            'codeValue',
            'description',
            'namespace'
        ],
        metaPrefix=None,
        recordPrefix='races_',
        errors='ignore'
    )
    races_descriptor_normalized["namespace_codevalue"] = races_descriptor_normalized['namespace'] + '#' + races_descriptor_normalized['codeValue']

    sex_descriptor_normalized = jsonNormalize(
        sex_descriptor_content,
        recordPath=None,
        meta=[
            'sexDescriptorId',
            'codeValue',
            'description',
            'namespace'
        ],
        metaPrefix=None,
        recordPrefix='sex_',
        errors='ignore'
    )
    sex_descriptor_normalized["namespace_codevalue"] = sex_descriptor_normalized['namespace'] + '#' + sex_descriptor_normalized['codeValue']

    candidates_normalized = jsonNormalize(
        candidates_content,
        recordPath=None,
        meta=[
            ['personReference', 'personId'],
            'candidateIdentifier',
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

    candidates_races_normalized = jsonNormalize(
        candidates_content,
        recordPath=['races'],
        meta=[
            'candidateIdentifier'
        ],
        metaPrefix=None,
        recordPrefix='candidates_races_',
        errors='ignore'
    )

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

    credentials_normalized = credentials_normalized.fillna('')
    addColumnIfNotExists(credentials_normalized, '_ext.tpdm.personReference.personId')
    credentials_normalized["hasPersonId"] = credentials_normalized['_ext.tpdm.personReference.personId'].astype(bool)
    credentials_normalized = credentials_normalized[credentials_normalized['hasPersonId'].astype(bool)]

    candidate_educator_preparation_program_associations_normalized = jsonNormalize(
        candidate_educator_preparation_program_associations_content,
        recordPath=None,
        meta=[
            'beginDate',
            'reasonExitedDescriptor',
            ['candidateReference', 'candidateIdentifier'],
            ['educatorPreparationProgramReference', 'programName'],
            ['educatorPreparationProgramReference', 'educationOrganizationId']
        ],
        metaPrefix=None,
        recordPrefix='candidate_educator_preparation_program_',
        errors='ignore'
    )

    replace_null(candidate_educator_preparation_program_associations_normalized, 'reasonExitedDescriptor', '')

    candidate_educator_preparation_program_associations_cohortyears_normalized = jsonNormalize(
        candidate_educator_preparation_program_associations_content,
        recordPath=[
            'cohortYears'
        ],
        meta=[
            ['candidateReference', 'candidateIdentifier'],
            ['educatorPreparationProgramReference', 'programName']],
        metaPrefix=None,
        recordPrefix='candidate_educator_preparation_program_',
        errors='ignore'
    )

    # Join student to person
    student_person_data_frame = pdMerge(
        left=students_normalized,
        right=people_normalized,
        how='inner',
        leftOn=['personReference.personId'],
        rightOn=['personId'],
        suffixLeft=None,
        suffixRight='_person'
    )

    candidates_normalized['candidate_prep_program'] = '|'

    result_data_frame = pdMerge(
        left=candidates_normalized,
        right=candidate_educator_preparation_program_associations_normalized,
        how='inner',
        leftOn=['candidateIdentifier'],
        rightOn=['candidateReference.candidateIdentifier'],
        suffixLeft=None,
        suffixRight="_candidate_educator_preparation_program_associations"
    )

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=candidate_educator_preparation_program_associations_cohortyears_normalized,
        how='left',
        leftOn=['candidateIdentifier', 'educatorPreparationProgramReference.programName'],
        rightOn=['candidateReference.candidateIdentifier', 'educatorPreparationProgramReference.programName'],
        suffixLeft=None,
        suffixRight="_candidate_prep_program_cohortyears"
    )

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=candidates_races_normalized,
        how='left',
        leftOn=['candidateIdentifier'],
        rightOn=['candidateIdentifier'],
        suffixLeft=None,
        suffixRight="_candidate_races"
    )

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=student_person_data_frame,
        how='left',
        leftOn=['personReference.personId'],
        rightOn=['personReference.personId'],
        suffixLeft=None,
        suffixRight='_student'
    )

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=credentials_normalized,
        how='left',
        leftOn=['personReference.personId'],
        rightOn=['_ext.tpdm.personReference.personId'],
        suffixLeft=None,
        suffixRight='_credentials'
    )

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=races_descriptor_normalized,
        how='left',
        leftOn=['candidates_races_raceDescriptor'],
        rightOn=['namespace_codevalue'],
        suffixLeft=None,
        suffixRight='_race'
    )

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=sex_descriptor_normalized,
        how='left',
        leftOn=['sexDescriptor'],
        rightOn=['namespace_codevalue'],
        suffixLeft=None,
        suffixRight='_sex'
    )

    replace_null(result_data_frame, 'candidate_educator_preparation_program_termDescriptor', '')

    result_data_frame = result_data_frame[
        [
            'candidateIdentifier',
            'firstName',
            'lastSurname',
            'sexDescriptorId',
            'codeValue_sex',
            'raceDescriptorId',
            'codeValue',
            'hispanicLatinoEthnicity',
            'economicDisadvantaged',
            'candidate_educator_preparation_program_schoolYearTypeReference.schoolYear',
            'studentUniqueId',
            'educatorPreparationProgramReference.programName',
            'beginDate',
            'educatorPreparationProgramReference.educationOrganizationId',
            'personReference.personId',
            'reasonExitedDescriptor',
            'candidate_educator_preparation_program_termDescriptor',
            'issuanceDate'
        ]]

    get_descriptor_code_value_from_uri(result_data_frame, 'reasonExitedDescriptor')
    get_descriptor_code_value_from_uri(result_data_frame, 'candidate_educator_preparation_program_termDescriptor')

    result_data_frame = result_data_frame.fillna('')

    result_data_frame['ProgramComplete'] = [1 if x == 'Completed' else 0 for x in result_data_frame['reasonExitedDescriptor']]

    result_data_frame.loc[result_data_frame['economicDisadvantaged'] == '', 'economicDisadvantaged'] = False
    result_data_frame["economicDisadvantaged_Int"] = result_data_frame["economicDisadvantaged"].astype(int)

    result_data_frame["hispanicLatinoEthnicity_Int"] = result_data_frame["hispanicLatinoEthnicity"].astype(int)

    result_data_frame = renameColumns(result_data_frame, {
        'candidateIdentifier': 'CandidateKey',
        'firstName': 'FirstName',
        'lastSurname': 'LastSurname',
        'sexDescriptorId': 'SexDescriptorKey',
        'codeValue_sex': 'SexDescriptor',
        'raceDescriptorId': 'RaceDescriptorKey',
        'codeValue': 'RaceDescriptor',
        'hispanicLatinoEthnicity_Int': 'HispanicLatinoEthnicity',
        'economicDisadvantaged_Int': 'EconomicDisadvantaged',
        'candidate_educator_preparation_program_schoolYearTypeReference.schoolYear': 'Cohort',
        'studentUniqueId': 'StudentKey',
        'educatorPreparationProgramReference.programName': 'ProgramName',
        'beginDate': 'BeginDate',
        'educatorPreparationProgramReference.educationOrganizationId': 'EducationOrganizationId',
        'personReference.personId': 'PersonId',
        'candidate_educator_preparation_program_termDescriptor': 'CohortYearTermDescription',
        'issuanceDate': 'IssuanceDate'
    })
    result_data_frame['RaceDescriptorKey'] = result_data_frame['RaceDescriptorKey'].astype(str)
    result_data_frame['Cohort'] = result_data_frame['Cohort'].astype(str)

    result_data_frame = result_data_frame.groupby([
        'CandidateKey'
        , 'FirstName'
        , 'LastSurname'
        , 'SexDescriptorKey'
        , 'SexDescriptor'
        , 'RaceDescriptorKey'
        , 'RaceDescriptor'
        , 'HispanicLatinoEthnicity'
        , 'EconomicDisadvantaged'
        , 'Cohort'
        , 'ProgramComplete'
        , 'StudentKey'
        , 'ProgramName'
        , 'BeginDate'
        , 'EducationOrganizationId'
        , 'PersonId'
        , 'CohortYearTermDescription'
    ], sort=False)['IssuanceDate'].min()

    result_data_frame = result_data_frame.to_frame()

    saveParquetFile(result_data_frame, f"{config('PARQUET_FILES_LOCATION')}", "epp_CandidateDim.parquet", school_year)

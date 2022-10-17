# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from datetime import date
from unittest import result

from decouple import config

from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    get_descriptor_code_value_from_uri,
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
    toCsv(races_descriptor_normalized, "C:/temp/edfi/parquet", "races_descriptor_normalized.csv", "")

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
    toCsv(sex_descriptor_normalized, "C:/temp/edfi/parquet", "sex_descriptor_normalized.csv", "")

    candidates_normalized = jsonNormalize(
        candidates_content,
        recordPath=None,
        meta=[
            ['personReference','personId'],
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
    toCsv(candidates_normalized, "C:/temp/edfi/parquet", "candidates_normalized.csv", "")

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
    toCsv(candidates_races_normalized, "C:/temp/edfi/parquet", "candidates_races_normalized.csv", "")

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
    toCsv(students_normalized, "C:/temp/edfi/parquet", "students_normalized.csv", "")

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
    toCsv(people_normalized, "C:/temp/edfi/parquet", "people_normalized.csv", "")

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
    credentials_normalized["hasPersonId"] = credentials_normalized['_ext.tpdm.personReference.personId'].astype(bool)
    credentials_normalized = credentials_normalized[credentials_normalized['hasPersonId'].astype(bool)]   

    toCsv(credentials_normalized, "C:/temp/edfi/parquet", "credentials_normalized.csv", "")

    candidate_educator_preparation_program_associations_normalized = jsonNormalize(
        candidate_educator_preparation_program_associations_content,
        recordPath=None,
        meta=['beginDate',
            'reasonExitedDescriptor',
            ['candidateReference','candidateIdentifier'],
            ['educatorPreparationProgramReference','programName'],
            ['educatorPreparationProgramReference','educationOrganizationId']],
        metaPrefix=None,
        recordPrefix='candidate_educator_preparation_program_',
        errors='ignore'
    )
    toCsv(candidate_educator_preparation_program_associations_normalized, "C:/temp/edfi/parquet", "candidate_educator_preparation_program_associations_normalized.csv", "")

    candidate_educator_preparation_program_associations_cohortyears_normalized = jsonNormalize(
        candidate_educator_preparation_program_associations_content,
        recordPath=[
            'cohortYears'
        ],
        meta=[
            ['candidateReference','candidateIdentifier'],
            ['educatorPreparationProgramReference','programName']],
        metaPrefix=None,
        recordPrefix='candidate_educator_preparation_program_',
        errors='ignore'
    )
    toCsv(candidate_educator_preparation_program_associations_cohortyears_normalized, "C:/temp/edfi/parquet", "candidate_educator_preparation_program_associations_cohortyears_normalized.csv", "")

    # Join student to person
    student_person_data_frame = pdMerge(
        left=students_normalized,
        right=people_normalized,
        how='inner',
        leftOn=['personReference.personId'],
        rigthOn=['personId'],
        suffixLeft=None,
        suffixRight='_person'
    )
    # toCsv(student_person_data_frame, "C:/temp/edfi/parquet", "student_person_data_frame.csv", "")

    candidates_normalized['candidate_prep_program'] = '|'

    result_data_frame = pdMerge(
        left=candidates_normalized,
        right=candidate_educator_preparation_program_associations_normalized,
        how='inner',
        leftOn=['candidateIdentifier'],
        rigthOn=['candidateReference.candidateIdentifier'],
        suffixLeft=None,
        suffixRight="_candidate_educator_preparation_program_associations"
    )
    # toCsv(result_data_frame, "C:/temp/edfi/parquet", "result_data_frame.csv", "")

    result_data_frame['candidate_prep_program_cohortyears'] = '|'

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=candidate_educator_preparation_program_associations_cohortyears_normalized,
        how='left',
        leftOn=['candidateIdentifier','educatorPreparationProgramReference.programName'],
        rigthOn=['candidateReference.candidateIdentifier','educatorPreparationProgramReference.programName'],
        suffixLeft=None,
        suffixRight="_candidate_prep_program_cohortyears"
    )
    # toCsv(result_data_frame, "C:/temp/edfi/parquet", "result_data_frame.csv", "")

    result_data_frame['candidate_races'] = '|'

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=candidates_races_normalized,
        how='left',
        leftOn=['candidateIdentifier'],
        rigthOn=['candidateIdentifier'],
        suffixLeft=None,
        suffixRight="_candidate_races"
    )
    # toCsv(result_data_frame, "C:/temp/edfi/parquet", "result_data_frame.csv", "")

    result_data_frame['student_people'] = '|'

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=student_person_data_frame,
        how='left',
        leftOn=['personReference.personId'],
        rigthOn=['personReference.personId'],
        suffixLeft=None,
        suffixRight='_student'
    )
    # toCsv(result_data_frame, "C:/temp/edfi/parquet", "result_data_frame_before.csv", "")

    result_data_frame['credentials'] = '|'

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=credentials_normalized,
        how='left',
        leftOn=['personReference.personId'],
        rigthOn=['_ext.tpdm.personReference.personId'],
        suffixLeft=None,
        suffixRight='_credentials'
    )
    # toCsv(result_data_frame, "C:/temp/edfi/parquet", "result_data_frame.csv", "")

    result_data_frame['races_descriptor'] = '|'

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=races_descriptor_normalized,
        how='left',
        leftOn=['candidates_races_raceDescriptor'],
        rigthOn=['namespace_codevalue'],
        suffixLeft=None,
        suffixRight='_race'
    )
    # toCsv(result_data_frame, "C:/temp/edfi/parquet", "result_data_frame.csv", "")

    result_data_frame['sex_descriptor'] = '|'

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=sex_descriptor_normalized,
        how='left',
        leftOn=['sexDescriptor'],
        rigthOn=['namespace_codevalue'],
        suffixLeft=None,
        suffixRight='_sex'
    )
    # toCsv(result_data_frame, "C:/temp/edfi/parquet", "result_data_frame.csv", "")

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
    # toCsv(result_data_frame, "C:/temp/edfi/parquet", "result_data_frame_before.csv", "")

    get_descriptor_code_value_from_uri(result_data_frame, 'reasonExitedDescriptor')
    get_descriptor_code_value_from_uri(result_data_frame, 'candidate_educator_preparation_program_termDescriptor')

    result_data_frame = result_data_frame.fillna('')

    result_data_frame['ProgramComplete'] = [True if x == 'Completed' else False for x in result_data_frame['reasonExitedDescriptor']]

    result_data_frame = renameColumns(result_data_frame, {
        'candidateIdentifier': 'CandidateKey',
        'firstName': 'FirstName',
        'lastSurname': 'LastSurname',
        'sexDescriptorId': 'SexDescriptorKey',
        'codeValue_sex': 'SexDescriptor',
        'raceDescriptorId': 'RaceDescriptorKey',
        'codeValue': 'RaceDescriptor',
        'hispanicLatinoEthnicity': 'HispanicLatinoEthnicity',
        'economicDisadvantaged': 'EconomicDisadvantaged',
        'candidate_educator_preparation_program_schoolYearTypeReference.schoolYear': 'Cohort',
        'studentUniqueId': 'StudentKey',
        'educatorPreparationProgramReference.programName': 'ProgramName',
        'beginDate': 'BeginDate',
        'educatorPreparationProgramReference.educationOrganizationId': 'EducationOrganizationId',
        'personReference.personId': 'PersonId',
        'candidate_educator_preparation_program_termDescriptor': 'CohortYearTermDescription',
        'issuanceDate': 'IssuanceDate'
    })
    
    result_data_frame.loc[result_data_frame['EconomicDisadvantaged'] == '', 'EconomicDisadvantaged'] = False

    result_data_frame['RaceDescriptorKey'] = result_data_frame['RaceDescriptorKey'].astype(str)
    result_data_frame['Cohort'] = result_data_frame['Cohort'].astype(str)

    result_data_frame = result_data_frame.groupby([
        'CandidateKey'
        ,'FirstName'
        ,'LastSurname'
        ,'SexDescriptorKey'
        ,'SexDescriptor'
        ,'RaceDescriptorKey'
        ,'RaceDescriptor'
        ,'HispanicLatinoEthnicity'
        ,'EconomicDisadvantaged'
        ,'Cohort'
        ,'ProgramComplete'
        ,'StudentKey'
        ,'ProgramName'
        ,'BeginDate'
        ,'EducationOrganizationId'
        ,'PersonId'
        ,'CohortYearTermDescription'
    ], sort=False)['IssuanceDate'].min()

    result_data_frame = result_data_frame.to_frame()

    toCsv(result_data_frame, "C:/temp/edfi/parquet", "result_data_frame.csv", "")

    saveParquetFile(result_data_frame, f"{config('PARQUET_FILES_LOCATION')}", "epp_CandidateDim.parquet", school_year)

# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from datetime import date

from decouple import config

from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    add_dataframe_column,
    get_descriptor_code_value_from_uri,
    jsonNormalize,
    pdMerge,
    renameColumns,
    saveParquetFile,
    subset,
    to_datetime_key,
)

ENDPOINT_STUDENT_ASSESSSMENTS = "studentAssessments"
ENDPOINT_STUDENT_SCHOOL_ASSOCIATION = "studentSchoolAssociations"


def student_assessment_fact_dataframe(school_year) -> None:
    silverDataLocation = config("SILVER_DATA_LOCATION")
    student_assessment_json = getEndpointJson(ENDPOINT_STUDENT_ASSESSSMENTS, silverDataLocation, school_year)
    student_school_association_json = getEndpointJson(ENDPOINT_STUDENT_SCHOOL_ASSOCIATION, silverDataLocation, school_year)
    ############################
    # studentSchoolAssociation
    ############################
    student_school_association_content = jsonNormalize(
        student_school_association_json,
        recordPath=None,
        meta=[
            'id',
            'entryDate',
            'schoolReference.schoolId',
            'studentReference.studentUniqueId',
            'exitWithdrawDate'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors="ignore",
    )
    student_school_association_content = subset(student_school_association_content, [
        'id',
        'entryDate',
        'schoolReference.schoolId',
        'studentReference.studentUniqueId',
        'exitWithdrawDate'
    ])
    ############################
    # student assessment
    ############################
    student_assessment_content = jsonNormalize(
        student_assessment_json,
        recordPath=None,
        meta=[
            'id',
            'administrationDate',
            'studentAssessmentIdentifier',
            'assessmentReference.assessmentIdentifier',
            'assessmentReference.namespace',
            'studentReference.studentUniqueId',
            'objectiveAssessmentReference.identificationCode'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors="ignore",
    )
    ############################
    # studentObjectiveAssessments
    ############################
    student_objective_assessment = jsonNormalize(
        student_assessment_json,
        recordPath=[
            'studentObjectiveAssessments'
        ],
        meta=['id'],
        metaPrefix=None,
        recordMeta=[
            'objectiveAssessmentReference.identificationCode',
        ],
        recordPrefix=None,
        errors='ignore'
    )
    student_assessment_score_results = jsonNormalize(
        student_assessment_json,
        recordPath=['scoreResults'],
        meta=['id'],
        metaPrefix=None,
        recordMeta=[
            'assessmentReportingMethodDescriptor',
            'result',
            'resultDatatypeTypeDescriptor'
        ],
        recordPrefix=None,
        errors='ignore'
    )
    student_assessment_performance_levels = jsonNormalize(
        student_assessment_json,
        recordPath=['performanceLevels'],
        meta=['id'],
        metaPrefix=None,
        recordMeta=[
            'assessmentReportingMethodDescriptor',
            'performanceLevelDescriptor'
            'performanceLevelMet'
        ],
        recordPrefix=None,
        errors='ignore'
    )
    student_objective_assessment_scoreResults = jsonNormalize(
        student_assessment_json,
        recordPath=['studentObjectiveAssessments', 'scoreResults'],
        meta=[
            'id',
            ['studentObjectiveAssessments', 'objectiveAssessmentReference', 'identificationCode']
        ],
        recordMeta=[
            'assessmentReportingMethodDescriptor',
            'result',
            'resultDatatypeTypeDescriptor'
        ],
        recordPrefix=None,
        errors='ignore'
    )
    student_objective_assessment_scoreResults = add_dataframe_column(
        student_objective_assessment_scoreResults,
        [
            'studentObjectiveAssessments.objectiveAssessmentReference.identificationCode'
        ]
    )
    student_objective_assessment_performanceLevels = jsonNormalize(
        student_assessment_json,
        recordPath=['studentObjectiveAssessments', 'performanceLevels'],
        meta=[
            'id',
            ['studentObjectiveAssessments', 'objectiveAssessmentReference', 'identificationCode']
        ],
        metaPrefix=None,
        recordMeta=[
            'assessmentReportingMethodDescriptor',
            'performanceLevelDescriptor',
            'performanceLevelMet'
        ],
        recordPrefix=None,
        errors='ignore'
    )
    student_objective_assessment_performanceLevels = add_dataframe_column(
        student_objective_assessment_performanceLevels,
        [
            'studentObjectiveAssessments.objectiveAssessmentReference.identificationCode'
        ]
    )
    data_frame = pdMerge(
        left=student_assessment_content,
        right=student_assessment_score_results,
        how='left',
        leftOn=['id'],
        rigthOn=['id'],
        suffixLeft=None,
        suffixRight=None
    )
    data_frame = pdMerge(
        left=data_frame,
        right=student_assessment_performance_levels,
        how='left',
        leftOn=['id'],
        rigthOn=['id'],
        suffixLeft=None,
        suffixRight='_performance_levels'
    )
    data_frame = pdMerge(
        left=data_frame,
        right=student_objective_assessment,
        how='left',
        leftOn=['id'],
        rigthOn=['id'],
        suffixLeft=None,
        suffixRight='_student_objective'
    )
    student_objective_assessment_performanceLevels = add_dataframe_column(
        student_objective_assessment_performanceLevels,
        [
            'objectiveAssessmentReference.identificationCode'
        ]
    )
    data_frame = pdMerge(
        left=data_frame,
        right=student_objective_assessment_performanceLevels,
        how='left',
        leftOn=[
            'id',
            'objectiveAssessmentReference.identificationCode'
        ],
        rigthOn=[
            'id',
            'studentObjectiveAssessments.objectiveAssessmentReference.identificationCode'
        ],
        suffixLeft=None,
        suffixRight='_student_objective_performanceLevels'
    )
    data_frame = pdMerge(
        left=data_frame,
        right=student_objective_assessment_scoreResults,
        how='left',
        leftOn=[
            'id',
            'objectiveAssessmentReference.identificationCode'
        ],
        rigthOn=[
            'id',
            'studentObjectiveAssessments.objectiveAssessmentReference.identificationCode'
        ],
        suffixLeft=None,
        suffixRight='_student_objective_scoreResults'
    )
    data_frame['s_student_school_association_content'] = '|'
    data_frame = pdMerge(
        left=data_frame,
        right=student_school_association_content,
        how='left',
        leftOn=['studentReference.studentUniqueId'],
        rigthOn=['studentReference.studentUniqueId'],
        suffixLeft=None,
        suffixRight='_student_school_association'
    )
    data_frame['exitWithdrawDate'] = to_datetime_key(data_frame, 'exitWithdrawDate')
    data_frame['date_now'] = date.today()
    data_frame['date_now'] = to_datetime_key(data_frame, 'date_now')
    data_frame = data_frame[data_frame['exitWithdrawDate'] >= data_frame['date_now']]
    get_descriptor_code_value_from_uri(data_frame, 'whenAssessedGradeLevelDescriptor')
    get_descriptor_code_value_from_uri(data_frame, 'assessmentReportingMethodDescriptor')
    get_descriptor_code_value_from_uri(data_frame, 'resultDatatypeTypeDescriptor')
    get_descriptor_code_value_from_uri(data_frame, 'assessmentReportingMethodDescriptor_performance_levels')
    get_descriptor_code_value_from_uri(data_frame, 'performanceLevelDescriptor')
    get_descriptor_code_value_from_uri(data_frame, 'assessmentReportingMethodDescriptor_student_objective_performanceLevels')
    get_descriptor_code_value_from_uri(data_frame, 'performanceLevelDescriptor_student_objective_performanceLevels')
    get_descriptor_code_value_from_uri(data_frame, 'assessmentReportingMethodDescriptor_student_objective_scoreResults')
    get_descriptor_code_value_from_uri(data_frame, 'resultDatatypeTypeDescriptor_student_objective_scoreResults')
    data_frame = subset(data_frame, [
        'id',
        'studentAssessmentIdentifier',
        'administrationDate',
        'whenAssessedGradeLevelDescriptor',
        'assessmentReference.assessmentIdentifier',
        'assessmentReference.namespace',
        'studentReference.studentUniqueId',
        'objectiveAssessmentReference.identificationCode',
        'assessmentReportingMethodDescriptor',
        'result',
        'resultDatatypeTypeDescriptor',
        'assessmentReportingMethodDescriptor_performance_levels',
        'performanceLevelDescriptor',
        'assessmentReportingMethodDescriptor_student_objective_performanceLevels',
        'performanceLevelDescriptor_student_objective_performanceLevels',
        'assessmentReportingMethodDescriptor_student_objective_scoreResults',
        'result_student_objective_scoreResults',
        'resultDatatypeTypeDescriptor_student_objective_scoreResults',
        'entryDate',
        'schoolReference.schoolId',
        'exitWithdrawDate'
    ])
    data_frame['StudentAssessmentFactKey'] = (
        data_frame['assessmentReference.assessmentIdentifier'] + '-'
        + data_frame['assessmentReference.namespace'] + '-'
        + data_frame['studentAssessmentIdentifier'] + '-'
        + data_frame['assessmentReportingMethodDescriptor'].astype(str) + '-'
        + data_frame['performanceLevelDescriptor'].astype(str) + '-'
        + data_frame['objectiveAssessmentReference.identificationCode'].astype(str) + '-'
        + data_frame['assessmentReportingMethodDescriptor_student_objective_scoreResults'].astype(str) + '-'
        + data_frame['performanceLevelDescriptor_student_objective_performanceLevels'].astype(str) + '-'
        + data_frame['studentReference.studentUniqueId'] + '-'
        + data_frame['schoolReference.schoolId'].astype(str) + '-'
        + data_frame['entryDate'].astype(str)
    )
    data_frame['StudentAssessmentKey'] = (
        data_frame['assessmentReference.assessmentIdentifier'] + '-'
        + data_frame['assessmentReference.namespace'] + '-'
        + data_frame['studentAssessmentIdentifier'] + '-'
        + data_frame['studentReference.studentUniqueId']
    )
    data_frame['AssessmentKey'] = (
        data_frame['assessmentReference.assessmentIdentifier'] + '-'
        + data_frame['assessmentReference.namespace']
    )
    data_frame['StudentSchoolKey'] = (
        data_frame['studentReference.studentUniqueId'] + '-'
        + data_frame['schoolReference.schoolId'].astype(str)
    )
    data_frame['StudentObjectiveAssessmentKey'] = (
        data_frame['studentReference.studentUniqueId'] + '-'
        + data_frame['objectiveAssessmentReference.identificationCode'] + '-'
        + data_frame['assessmentReference.assessmentIdentifier'] + '-'
        + data_frame['studentAssessmentIdentifier'] + '-'
        + data_frame['assessmentReference.namespace']
    )
    data_frame['ObjectiveAssessmentKey'] = (
        + data_frame['assessmentReference.assessmentIdentifier'] + '-'
        + data_frame['objectiveAssessmentReference.identificationCode'] + '-'
        + data_frame['assessmentReference.namespace']
    )
    data_frame = renameColumns(data_frame, {
        'assessmentReference.assessmentIdentifier': 'AssessmentIdentifier',
        'assessmentReference.namespace': 'Namespace',
        'studentAssessmentIdentifier': 'StudentAssessmentIdentifier',
        'studentReference.studentUniqueId': 'StudentKey',
        'schoolReference.schoolId': 'SchoolKey',
        'administrationDate': 'AdministrationDate',
        'whenAssessedGradeLevelDescriptor': 'AssessedGradeLevel',
        'result_student_objective_scoreResults': 'StudentScore',
        'resultDatatypeTypeDescriptor_student_objective_scoreResults': 'ResultDataType',
        'assessmentReportingMethodDescriptor_student_objective_scoreResults': 'ReportingMethod',
        'performanceLevelDescriptor_student_objective_performanceLevels': 'PerformanceResult'
    })
    data_frame = data_frame.fillna('')
    # Result columns
    data_frame.loc[data_frame['StudentScore'] == '', 'StudentScore'] = data_frame['result']
    data_frame.loc[data_frame['ResultDataType'] == '', 'ResultDataType'] = data_frame['resultDatatypeTypeDescriptor']
    data_frame.loc[data_frame['ReportingMethod'] == '', 'ReportingMethod'] = data_frame['assessmentReportingMethodDescriptor']
    data_frame.loc[data_frame['PerformanceResult'] == '', 'PerformanceResult'] = data_frame['performanceLevelDescriptor']
    data_frame["StudentAssessmentScore"] = data_frame['result']
    data_frame["StudentAssessmentResultDataType"] = data_frame['resultDatatypeTypeDescriptor']
    data_frame["StudentAssessmentReportingMethod"] = data_frame['assessmentReportingMethodDescriptor']
    data_frame["StudentAssessmentPerformanceResult"] = data_frame['performanceLevelDescriptor']
    # Result dataframe
    data_frame = data_frame[[
        'StudentAssessmentFactKey',
        'StudentAssessmentKey',
        'StudentObjectiveAssessmentKey',
        'ObjectiveAssessmentKey',
        'AssessmentKey',
        'AssessmentIdentifier',
        'Namespace',
        'StudentAssessmentIdentifier',
        'StudentKey',
        'StudentSchoolKey',
        'SchoolKey',
        'AdministrationDate',
        'AssessedGradeLevel',
        'StudentScore',
        'ResultDataType',
        'ReportingMethod',
        'PerformanceResult',
        'StudentAssessmentScore',
        'StudentAssessmentResultDataType',
        'StudentAssessmentReportingMethod',
        'StudentAssessmentPerformanceResult'
    ]]
    return data_frame


def student_assessment_fact(school_year) -> None:
    result_data_frame = student_assessment_fact_dataframe(school_year)
    saveParquetFile(result_data_frame, f"{config('PARQUET_FILES_LOCATION')}", "asmt_student_assessment_fact.parquet", school_year)

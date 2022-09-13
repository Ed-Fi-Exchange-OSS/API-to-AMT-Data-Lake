# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import os
from decouple import config
from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import jsonNormalize, pdMerge, subset, saveParquetFile
from edfi_amt_data_lake.parquet.amt.base.SchoolDim.script import ENDPOINT_SCHOOLS
from edfi_amt_data_lake.parquet.amt.ews.StudentEarlyWarningFactDim.script import ENDPOINT_STUDENT_SCHOOL_ASSOCIATION

ENDPOINT_STUDENT_ASSESSSMENTS = 'studentAssessments'

def assessment_student(school_year) -> None:
    silverDataLocation = config('SILVER_DATA_LOCATION')
    school_json = getEndpointJson(ENDPOINT_SCHOOLS, silverDataLocation,school_year)
    student_assessment_json = getEndpointJson(ENDPOINT_STUDENT_ASSESSSMENTS, silverDataLocation,school_year)
    student_school_association_json = getEndpointJson(ENDPOINT_STUDENT_SCHOOL_ASSOCIATION, silverDataLocation,school_year)

    student_assesssment_content = jsonNormalize(
        student_assessment_json,
        recordPath=None,
        meta=[],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
      )

    student_school_association_content = jsonNormalize(
        student_school_association_json,
        recordPath=None,
        meta=None,
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
      )

    data_frame = pdMerge(
        left=student_assesssment_content, 
        right=student_school_association_content,
        how='left',
        leftOn=['studentReference.studentUniqueId'],
        rigthOn=['studentReference.studentUniqueId'],
        suffixLeft='_left',
        suffixRight=None
    )

    student_assessment_reporting_method_descriptor = jsonNormalize(
        student_assessment_json,
        recordPath=['scoreResults'],
        meta=['id',
            ['assessmentReportingMethodDescriptor',
             'resultDatatypeTypeDescriptor', 'result']],
        metaPrefix='_',
        recordPrefix=None,
        errors='ignore'
    )

    data_frame = pdMerge(
        left=data_frame, 
        right=student_assessment_reporting_method_descriptor,
        how='left',
        leftOn=['id_left'],
        rigthOn=['_id'],
        suffixLeft=None,
        suffixRight=None
    )

    performance_level = jsonNormalize(
         student_assessment_json,
         recordPath=['performanceLevels'],
         meta=['id','performanceLevelDescriptor'],
         metaPrefix='_',
         recordPrefix=None,
         errors='ignore'
     )

    data_frame = pdMerge(
         left=data_frame, 
         right=performance_level,
         how='left',
         leftOn=None,
         rigthOn=None,
         suffixLeft=None,
         suffixRight=None
     )

    school_content = jsonNormalize(
        school_json,
        ['addresses'],
        ['schoolId', 'nameOfInstitution', 'schoolTypeDescriptor', ['localEducationAgencyReference','localEducationAgencyId' ]],
        None,
        'address',
        'ignore'
    )

    data_frame = pdMerge(
         left=data_frame, 
         right=school_content,
         how='left',
         leftOn=['schoolReference.schoolId'],
         rigthOn=['schoolId'],
         suffixLeft=None,
         suffixRight=None
     )

    data_frame = data_frame.fillna('')
 
    data_frame['PerformanceResult'] = ('')

    if not data_frame['whenAssessedGradeLevelDescriptor'].empty:
        if len(data_frame['whenAssessedGradeLevelDescriptor'].str.split('#')) > 0:
            data_frame['whenAssessedGradeLevelDescriptor'] = data_frame['whenAssessedGradeLevelDescriptor'].str.split("#").str.get(1)

    if not data_frame['resultDatatypeTypeDescriptor'].empty:
        if len(data_frame['resultDatatypeTypeDescriptor'].str.split('#')) > 0:
            data_frame['resultDatatypeTypeDescriptor'] = data_frame['resultDatatypeTypeDescriptor'].str.split("#").str.get(1)

    if not data_frame['assessmentReportingMethodDescriptor'].empty:
        if len(data_frame['assessmentReportingMethodDescriptor'].str.split('#')) > 0:
            data_frame['assessmentReportingMethodDescriptor'] = data_frame['assessmentReportingMethodDescriptor'].str.split("#").str.get(1)

    data_frame['StudentAssessmentFactKey'] = (
         data_frame['assessmentReference.assessmentIdentifier'] + '-'
        + data_frame['assessmentReference.namespace'] + '-'
        + data_frame['studentAssessmentIdentifier'] + '-'
        + data_frame['assessmentReportingMethodDescriptor'] + '-'
        + data_frame['performanceLevelDescriptor']  + '-'
        + data_frame['studentAssessmentIdentifier'] + '-'
        + data_frame['assessmentReportingMethodDescriptor'] + '-'
        + data_frame['performanceLevelDescriptor'] + '-'
        + data_frame['studentReference.studentUniqueId'] + '-'
        + str(data_frame['schoolReference.schoolId'][0]) + '-'
        + str(data_frame['entryDate'][0]).replace('-','') 
    )

    data_frame['StudentAssessmentKey'] = (
        data_frame['assessmentReference.assessmentIdentifier'] + '-'
        + data_frame['assessmentReference.namespace'] + '-'
        + data_frame['studentAssessmentIdentifier'] + '-'
        + data_frame['studentReference.studentUniqueId']
    )

    data_frame['StudentObjectiveAssessmentKey'] = (
        data_frame['studentReference.studentUniqueId'] + '-'
        + data_frame['assessmentReference.assessmentIdentifier'] + '-'
        + data_frame['studentAssessmentIdentifier'] + '-'
        + data_frame['assessmentReference.namespace']
    )

    data_frame['ObjectiveAssessmentKey'] = (
        data_frame['studentAssessmentIdentifier'] + '-'
        + data_frame['assessmentReference.assessmentIdentifier'] + '-'
        + data_frame['assessmentReference.namespace']
    )

    data_frame['Assessment'] = (
        data_frame['assessmentReference.assessmentIdentifier'] + '-'
        + data_frame['assessmentReference.namespace']
    )

    data_frame['AssessmentIdentifier'] = (
        data_frame['assessmentReference.assessmentIdentifier']
    )

    data_frame['Namespace'] = (
        data_frame['assessmentReference.namespace']
    )

    data_frame['StudentAssessmentIdentifier'] = (
        data_frame['studentAssessmentIdentifier']
    )

    data_frame['StudentUSI'] = (
        data_frame['studentReference.studentUniqueId']
    )

    data_frame['StudentSchoolKey'] = (
        data_frame['studentReference.studentUniqueId'] + '-'
        + str(data_frame['schoolReference.schoolId'][0])
    )

    data_frame['SchoolKey'] = (
        data_frame['schoolReference.schoolId']
    )

    data_frame['AdministrationDate'] = (
        str(data_frame['administrationDate']).split('T')[0].replace('-','')
    )
    data_frame['AssessedGradeLevel'] = (
        data_frame['whenAssessedGradeLevelDescriptor']
    )

    data_frame['StudentScore'] = (
        data_frame['result']
    )

    data_frame['ResultDataType'] = (
        data_frame['resultDatatypeTypeDescriptor']
    )

    data_frame['ReportingMethod'] = (
        data_frame['assessmentReportingMethodDescriptor']
    )

    data_frame = subset(data_frame, [
           'StudentAssessmentFactKey',
           'StudentAssessmentKey',
           'StudentObjectiveAssessmentKey',
           'ObjectiveAssessmentKey',
           'Assessment',
           'AssessmentIdentifier',
           'Namespace',
           'StudentAssessmentIdentifier',
           'StudentUSI',
           'StudentSchoolKey',
           'SchoolKey',
           'AdministrationDate',
           'AssessedGradeLevel',
           'StudentScore',
           'ResultDataType',
           'ReportingMethod',
           'PerformanceResult'])

    if not os.path.exists(f"{config('PARQUET_FILES_LOCATION')}"):
        os.makedirs(f"{config('PARQUET_FILES_LOCATION')}")

    saveParquetFile(data_frame, f"{config('PARQUET_FILES_LOCATION')}/asmt_student_assessment_fact.parquet")


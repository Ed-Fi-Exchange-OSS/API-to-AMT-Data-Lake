# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from distutils.util import subst_vars
from operator import contains
from decouple import config
from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import jsonNormalize, pdMerge, subset, renameColumns, saveParquetFile, addColumnIfNotExists, toCsv, toDateTime

ENDPOINT_GRADES = 'grades'
GRANDINGPERIODS_GRADES = 'gradingPeriods'

def StudentSectionGradeFact(school_year="") -> None:
    gradesContent = getEndpointJson(ENDPOINT_GRADES, config('SILVER_DATA_LOCATION'), school_year)
    gradingPeriodsContent = getEndpointJson(GRANDINGPERIODS_GRADES, config('SILVER_DATA_LOCATION'), school_year)

    gradesContentNormalized =  jsonNormalize(
        gradesContent,
        None,
        None,
        None,
        None,
        'ignore'
    )

    gradingPeriodsContentNormalized =  jsonNormalize(
        gradingPeriodsContent,
        None,
        None,
        None,
        None,
        'ignore'
    )

    restultDataFrame = pdMerge(
        left=gradesContentNormalized, 
        right=gradingPeriodsContentNormalized,
        how='left',
        leftOn=['gradingPeriodReference.gradingPeriodDescriptor','gradingPeriodReference.periodSequence','gradingPeriodReference.schoolId','gradingPeriodReference.schoolYear'],
        rigthOn=['gradingPeriodDescriptor','periodSequence','schoolReference.schoolId','schoolYearTypeReference.schoolYear'],
        suffixLeft='',
        suffixRight='_gradingPeriods'
    )

    addColumnIfNotExists(restultDataFrame, 'letterGradeEarned')
    
    # Keep the fields I actually need.
    restultDataFrame = subset(restultDataFrame, [
        'studentSectionAssociationReference.studentUniqueId',
        'studentSectionAssociationReference.schoolId',
        'gradingPeriodReference.gradingPeriodDescriptor',
        'studentSectionAssociationReference.beginDate',
        'studentSectionAssociationReference.localCourseCode',
        'studentSectionAssociationReference.schoolYear',
        'studentSectionAssociationReference.sectionIdentifier',
        'studentSectionAssociationReference.sessionName',
        'numericGradeEarned',
        'letterGradeEarned',
        'gradeTypeDescriptor'
    ])

    # Formatting begin date that will be used as part of the keys later
    restultDataFrame['studentSectionAssociationReference.beginDate'] = toDateTime(restultDataFrame['studentSectionAssociationReference.beginDate'])
    restultDataFrame['studentSectionAssociationReference.beginDate'] = restultDataFrame['studentSectionAssociationReference.beginDate'].dt.strftime('%Y%m%d')

    # # Removes namespace from Grading Period Descriptor
    if not restultDataFrame['gradingPeriodReference.gradingPeriodDescriptor'].empty:
        if len(restultDataFrame['gradingPeriodReference.gradingPeriodDescriptor'].str.split('#')) > 0:
            restultDataFrame["gradingPeriodReference.gradingPeriodDescriptor"] = restultDataFrame["gradingPeriodReference.gradingPeriodDescriptor"].str.split("#").str.get(1)

    # Removes namespace from Grade Type Descriptor
    if not restultDataFrame['gradeTypeDescriptor'].empty:
        if len(restultDataFrame['gradeTypeDescriptor'].str.split('#')) > 0:
            restultDataFrame["gradeTypeDescriptor"] = restultDataFrame["gradeTypeDescriptor"].str.split("#").str.get(1)

    # Converting some fields to str as preparation for the parquet file.
    restultDataFrame['studentSectionAssociationReference.schoolId'] = restultDataFrame['studentSectionAssociationReference.schoolId'].astype(str)
    restultDataFrame['studentSectionAssociationReference.schoolYear'] = restultDataFrame['studentSectionAssociationReference.schoolYear'].astype(str)

    # Creates concatanation for GradingPeriodKey field
    restultDataFrame['GradingPeriodKey'] = (
            restultDataFrame['gradingPeriodReference.gradingPeriodDescriptor']
            + '-' + restultDataFrame['studentSectionAssociationReference.schoolId']
            + '-' + restultDataFrame['studentSectionAssociationReference.beginDate']
        )

    # Creates concatanation for StudentSectionKey field
    restultDataFrame['StudentSectionKey'] = (
            restultDataFrame['studentSectionAssociationReference.studentUniqueId']
            + '-' + restultDataFrame['studentSectionAssociationReference.schoolId']
            + '-' + restultDataFrame['studentSectionAssociationReference.localCourseCode']
            + '-' + restultDataFrame['studentSectionAssociationReference.schoolYear']
            + '-' + restultDataFrame['studentSectionAssociationReference.sectionIdentifier']
            + '-' + restultDataFrame['studentSectionAssociationReference.sessionName']
            + '-' + restultDataFrame['studentSectionAssociationReference.beginDate']
        )

    # Creates concatanation for SectionKey field
    restultDataFrame['SectionKey'] = (
            restultDataFrame['studentSectionAssociationReference.schoolId']
            + '-' + restultDataFrame['studentSectionAssociationReference.localCourseCode']
            + '-' + restultDataFrame['studentSectionAssociationReference.schoolYear']
            + '-' + restultDataFrame['studentSectionAssociationReference.sectionIdentifier']
            + '-' + restultDataFrame['studentSectionAssociationReference.sessionName']
        )

    # Rename columns to match AMT
    restultDataFrame = renameColumns(restultDataFrame, 
        {
            'studentSectionAssociationReference.studentUniqueId': 'StudentKey',
            'studentSectionAssociationReference.schoolId': 'SchoolKey',
            'numericGradeEarned': 'NumericGradeEarned',
            'letterGradeEarned': 'LetterGradeEarned',
            'gradeTypeDescriptor': 'GradeType'
        })

    # Reorder columns to match AMT
    restultDataFrame = restultDataFrame[[
            'StudentKey',
            'SchoolKey',
            'GradingPeriodKey',
            'StudentSectionKey',
            'SectionKey',
            'NumericGradeEarned',
            'LetterGradeEarned',
            'GradeType'
        ]]

    saveParquetFile(restultDataFrame, f"{config('PARQUET_FILES_LOCATION')}studentSectionGradeFact.parquet")
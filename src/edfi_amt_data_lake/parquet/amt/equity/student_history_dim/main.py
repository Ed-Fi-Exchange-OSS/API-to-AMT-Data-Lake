# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from datetime import date

from decouple import config
from edfi_amt_data_lake.helper.data_frame_generation_result import (
    data_frame_generation_result,
)
from edfi_amt_data_lake.parquet.amt.base.all_student_school_dim.main import (
    all_student_school_dim,
)
from edfi_amt_data_lake.parquet.amt.chrab.chronic_absenteeism_attendance_fact.main import (
    chronic_absenteeism_attendance_fact,
)
from edfi_amt_data_lake.parquet.amt.equity.student_discipline_action_dim.main import (
    student_discipline_action_dim,
)
from edfi_amt_data_lake.parquet.amt.base.school_dim.main import (
    school_dim,
)
from edfi_amt_data_lake.parquet.amt.base.student_school_dim.main import (
    student_school_dim,
)
from edfi_amt_data_lake.parquet.amt.base.student_section_dim.main import (
    student_section_dim,
)
from edfi_amt_data_lake.parquet.Common.descriptor_mapping import get_descriptor_constant
from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    create_empty_data_frame,
    get_descriptor_code_value_from_uri,
    jsonNormalize,
    pdMerge,
    renameColumns,
    replace_null,
    toCsv,
    subset,
    to_datetime_key,
)

ENDPOINT_STUDENT_GRADES_ASSOCIATION = 'grades'
ENDPOINT_SCHOOL = 'schools'

@create_parquet_file
def student_history_dim_data_frame(
    file_name: str,
    columns: list[str],
    school_year: int
):
    student_grades_association_content = getEndpointJson(ENDPOINT_STUDENT_GRADES_ASSOCIATION, config('SILVER_DATA_LOCATION'), school_year)
    chronic_absenteeism_attendance_fact_view = chronic_absenteeism_attendance_fact(school_year).data_frame
    school_dim_view = school_dim(school_year).data_frame
    student_school_dim_view = student_school_dim(school_year).data_frame
    student_section_dim_view = student_section_dim(school_year).data_frame
    student_enrollment_dim_view = all_student_school_dim(school_year).data_frame
    student_discipline_dim_view = student_discipline_action_dim(school_year).data_frame
    ############################
    # Student Enrollment
    ############################
    student_enrollment_dim_view = pdMerge(
        left=student_enrollment_dim_view,
        right=school_dim_view,
        how='inner',
        leftOn=['SchoolKey'],
        rightOn=['SchoolKey'],
        suffixLeft=None,
        suffixRight=None
    )
    student_enrollment_dim_view = subset(
        student_enrollment_dim_view,
        [
            'StudentSchoolKey',
            'IsEnrolled',
            'ExitWithdrawDate',
            'SchoolName'
        ]
    )    
    student_enrollment_dim_view['ExitWithdrawDateKey'] = student_enrollment_dim_view['ExitWithdrawDate']
    student_enrollment_dim_view = replace_null(
        student_enrollment_dim_view,
        'ExitWithdrawDate',
        '2099-12-31'
    )
    student_enrollment_dim_view['ExitWithdrawDateKey'] = to_datetime_key(student_enrollment_dim_view, 'ExitWithdrawDateKey')
    student_enrollment_dim_view['schoolEnrollmentDate'] = (
        student_enrollment_dim_view['SchoolName'] + ' ' + student_enrollment_dim_view['ExitWithdrawDate']
        if (len(student_enrollment_dim_view['ExitWithdrawDate']) >= 6) else student_enrollment_dim_view['SchoolName']
    )    
    student_enrollment_dim_view.sort_values(by=[
            'StudentSchoolKey',
            'IsEnrolled',
            'ExitWithdrawDateKey',
            'schoolEnrollmentDate'
        ], inplace=True,
        ascending=False
    )
    student_enrollment_dim_view = (
        student_enrollment_dim_view.groupby(
            ['StudentSchoolKey'], 
            as_index=False).agg(EnrollmentHistory={'schoolEnrollmentDate': '\n '.join}
        )
    )
    student_enrollment_dim_view = subset(
        student_enrollment_dim_view,
        [
            'StudentSchoolKey',
            'EnrollmentHistory'
        ]
    )
    ############################
    # Attendance History
    ############################
    attendance_history = (
        chronic_absenteeism_attendance_fact_view.groupby(
            'StudentSchoolKey'
        ).agg(
            DaysEnrolled=('StudentSchoolKey', 'count'), 
            DaysAbsent=('ReportedAsAbsentFromHomeRoom', 'sum')
        )
    )
    attendance_history = subset(
        attendance_history,
        [
            'StudentSchoolKey',
            'DaysEnrolled',
            'DaysAbsent'
        ]
    )
    ############################
    # Discipline Action
    ############################
    discipline_action = (
        student_discipline_dim_view.groupby(['StudentSchoolKey']).agg(
            ReferralsAndSuspensions=('StudentSchoolKey', 'count')
        )
    )
    discipline_action = subset(
        discipline_action,
        [
            'StudentSchoolKey',
            'ReferralsAndSuspensions'
        ]
    )
    ############################
    # Student Grades Summary
    ############################
    student_grades_association_normalized = jsonNormalize(
        student_grades_association_content,
        recordPath=None,
        meta=[
            'studentSectionAssociationReference.localCourseCode',
            'studentSectionAssociationReference.schoolYear',
            'studentSectionAssociationReference.sectionIdentifier',
            'studentSectionAssociationReference.sessionName',
            'studentSectionAssociationReference.studentUniqueId',
            'studentSectionAssociationReference.schoolId',
            'numericGradeEarned',
            'letterGradeEarned',
            'gradeTypeDescriptor'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )
    student_grades_association_normalized = renameColumns(
        student_grades_association_normalized,
        {
            'studentSectionAssociationReference.studentUniqueId': 'StudentKey',
            'studentSectionAssociationReference.schoolId': 'SchoolKey'
        }
    )
    # Get Descriptor
    get_descriptor_code_value_from_uri(
        student_grades_association_normalized,
        'gradeTypeDescriptor'
    )
    student_grades_association_normalized['StudentSchoolKey'] = (
        student_grades_association_normalized['StudentKey'].astype(str)
        + '-' + student_grades_association_normalized['SchoolKey'].astype(str)
    )
    student_grades_association_normalized['StudentSectionKey'] = (
        student_grades_association_normalized['StudentKey'].astype(str)
        + '-' + student_grades_association_normalized['SchoolKey'].astype(str)
        + '-' + student_grades_association_normalized['studentSectionAssociationReference.localCourseCode'].astype(str)
        + '-' + student_grades_association_normalized['studentSectionAssociationReference.schoolYear'].astype(str)
        + '-' + student_grades_association_normalized['studentSectionAssociationReference.sectionIdentifier'].astype(str)
        + '-' + student_grades_association_normalized['studentSectionAssociationReference.sessionName'].astype(str)
    )
    student_grades_association_normalized = subset(
        student_grades_association_normalized,
        [
            'StudentSectionKey',
            'gradeTypeDescriptor',
            'numericGradeEarned'
        ]
    )
    student_section_dim_view = subset(
        student_section_dim_view,
        [
            'StudentSectionKey',
            'StudentKey',
            'StudentSchoolKey',
            'CourseTitle'
        ]
    )
    student_grades_association_normalized = pdMerge(
        left=student_grades_association_normalized,
        right=student_section_dim_view,
        how='inner',
        leftOn=['StudentSectionKey'],
        rightOn=['StudentSectionKey'],
        suffixLeft=None,
        suffixRight=None
    )
    if not (student_grades_association_normalized is None):
        student_grades_association_normalized['GradeSummary'] = (
                student_grades_association_normalized['CourseTitle']
                + ' ' + student_grades_association_normalized['numericGradeEarned']
            )
        student_grades_association_normalized = (
            student_grades_association_normalized.groupby(
                ['StudentSchoolKey'], 
                as_index=False).agg({'courseGrade': '\n '.join}
            )
        )
        student_grades_association_normalized = get_descriptor_constant(student_grades_association_normalized, 'gradeTypeDescriptor')
        student_grades_association_normalized = (
            student_grades_association_normalized[
                student_grades_association_normalized[
                    "gradeTypeDescriptor_constantName"
                ].str.contains('GradeType.Semester', na=False)
            ]
        )
        student_grades_association_normalized = subset(
            student_grades_association_normalized,
            [
                'StudentSchoolKey',
                'GradeSummary'
            ]
        )
    else:
        student_grades_association_normalized = create_empty_data_frame(
            'StudentSchoolKey',
            'GradeSummary'

        )    
    ############################
    # Student enrollment
    ############################
    result_data_frame = pdMerge(
        left=student_school_dim,
        right=student_enrollment_dim_view,
        how='inner',
        leftOn=['StudentSchoolKey'],
        rightOn=['StudentSchoolKey'],
        suffixLeft=None,
        suffixRight=None
    )
    if result_data_frame is None:
        return None
    result_data_frame = pdMerge(
        left=student_school_dim,
        right=attendance_history,
        how='left',
        leftOn=['StudentSchoolKey'],
        rightOn=['StudentSchoolKey'],
        suffixLeft=None,
        suffixRight=None
    )
    result_data_frame = pdMerge(
        left=student_school_dim,
        right=discipline_action,
        how='left',
        leftOn=['StudentSchoolKey'],
        rightOn=['StudentSchoolKey'],
        suffixLeft=None,
        suffixRight=None
    )
    result_data_frame = pdMerge(
        left=student_school_dim,
        right=student_grades_association_normalized,
        how='left',
        leftOn=['StudentSchoolKey'],
        rightOn=['StudentSchoolKey'],
        suffixLeft=None,
        suffixRight=None
    )
    toCsv(result_data_frame, f"{config('PARQUET_FILES_LOCATION')}", "equity_StudentHistoryDim.csv", school_year)
    return result_data_frame

def student_history_dim(school_year) -> data_frame_generation_result:
    return student_history_dim_data_frame(
        file_name="equity_StudentHistoryDim.parquet",
        columns=[],#RESULT_COLUMNS,
        school_year=school_year
    )    

    

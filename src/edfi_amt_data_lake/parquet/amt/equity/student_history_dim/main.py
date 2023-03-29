# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import pandas as pd
from decouple import config

from edfi_amt_data_lake.helper.data_frame_generation_result import (
    data_frame_generation_result,
)
from edfi_amt_data_lake.parquet.amt.base.all_student_school_dim.main import (
    all_student_school_dim,
)
from edfi_amt_data_lake.parquet.amt.base.school_dim.main import school_dim
from edfi_amt_data_lake.parquet.amt.base.student_school_dim.main import (
    student_school_dim,
)
from edfi_amt_data_lake.parquet.amt.base.student_section_dim.main import (
    student_section_dim,
)
from edfi_amt_data_lake.parquet.amt.chrab.chronic_absenteeism_attendance_fact.main import (
    chronic_absenteeism_attendance_fact,
)
from edfi_amt_data_lake.parquet.amt.equity.student_discipline_action_dim.main import (
    student_discipline_action_dim,
)
from edfi_amt_data_lake.parquet.Common.descriptor_mapping import get_descriptor_constant
from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    addColumnIfNotExists,
    create_empty_data_frame,
    create_parquet_file,
    is_data_frame_empty,
    jsonNormalize,
    pdMerge,
    renameColumns,
    replace_null,
    replace_null_empty,
    subset,
    to_datetime_key,
)

ENDPOINT_STUDENT_GRADES_ASSOCIATION = 'grades'
ENDPOINT_SCHOOL = 'schools'
RESULT_COLUMNS = [
    'StudentKey',
    'StudentSchoolKey',
    'GradeSummary',
    'CurrentSchoolKey',
    'AttendanceRate',
    'ReferralsAndSuspensions',
    'EnrollmentHistory'
]


@create_parquet_file
def student_history_dim_data_frame(
    file_name: str,
    columns: list[str],
    school_year: int
):
    file_name = file_name
    student_grades_association_content = getEndpointJson(ENDPOINT_STUDENT_GRADES_ASSOCIATION, config('SILVER_DATA_LOCATION'), school_year)
    chronic_absenteeism_attendance_fact_view = chronic_absenteeism_attendance_fact(school_year).data_frame
    school_dim_view = school_dim(school_year).data_frame
    student_school_dim_view = student_school_dim(school_year).data_frame
    student_section_dim_view = student_section_dim(school_year).data_frame
    student_enrollment_dim_view = all_student_school_dim(school_year).data_frame
    student_discipline_dim_view = student_discipline_action_dim(school_year).data_frame
    if is_data_frame_empty(student_school_dim_view) or is_data_frame_empty(student_enrollment_dim_view):
        return None
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
            'StudentKey',
            'IsEnrolled',
            'ExitWithdrawDate',
            'SchoolName'
        ]
    )
    student_enrollment_dim_view['ExitWithdrawDateKey'] = to_datetime_key(student_enrollment_dim_view, 'ExitWithdrawDate')
    student_enrollment_dim_view['EnrollmentHistory'] = (
        student_enrollment_dim_view['SchoolName'] + ' ' + student_enrollment_dim_view['ExitWithdrawDate']
        if (len(student_enrollment_dim_view['ExitWithdrawDate']) >= 6) else student_enrollment_dim_view['SchoolName']
    )
    student_enrollment_dim_view = student_enrollment_dim_view.drop_duplicates()
    student_enrollment_dim_view.sort_values(
        by=[
            'StudentKey',
            'IsEnrolled',
            'ExitWithdrawDateKey',
            'EnrollmentHistory'
        ],
        inplace=True,
        ascending=False
    )
    student_enrollment_dim_view = (
        student_enrollment_dim_view.groupby(
            ['StudentKey'],
            as_index=False,
            group_keys=True
        ).agg({'EnrollmentHistory': ' \n'.join})
    )
    student_enrollment_dim_view = subset(
        student_enrollment_dim_view,
        [
            'StudentKey',
            'EnrollmentHistory'
        ]
    )
    ############################
    # Attendance History
    ############################
    chronic_absenteeism_attendance_fact_view = subset(
        chronic_absenteeism_attendance_fact_view,
        [
            'StudentSchoolKey',
            'ReportedAsAbsentFromHomeRoom'
        ]
    )
    attendance_history = None
    if not chronic_absenteeism_attendance_fact_view.empty:
        attendance_history = (
            chronic_absenteeism_attendance_fact_view.groupby(
                ['StudentSchoolKey'],
                as_index=False,
                group_keys=False
            ).apply(lambda s: pd.Series({
                'DaysEnrolled': s['ReportedAsAbsentFromHomeRoom'].count(),
                'DaysAbsent': s['ReportedAsAbsentFromHomeRoom'].sum(),
            }))
        )
        attendance_history.reset_index()
        addColumnIfNotExists(attendance_history, 'DaysEnrolled', '1')
        addColumnIfNotExists(attendance_history, 'DaysAbsent', '0')
        replace_null_empty(attendance_history, 'DaysEnrolled', 1)
        replace_null_empty(attendance_history, 'DaysAbsent', 0)
        attendance_history['AttendanceRate'] = (
            100
            * (
                attendance_history['DaysEnrolled'].astype(int)
                - attendance_history['DaysAbsent'].astype(int)
            )
            / attendance_history['DaysEnrolled'].astype(int)
        ).astype(float)
    else:
        attendance_history = create_empty_data_frame(
            [
                'StudentSchoolKey',
                'AttendanceRate'
            ]
        )
    attendance_history = subset(
        attendance_history,
        [
            'StudentSchoolKey',
            'AttendanceRate'
        ]
    ).reset_index()

    if is_data_frame_empty(attendance_history):
        return None
    addColumnIfNotExists(attendance_history, 'AttendanceRate')
    replace_null_empty(attendance_history, 'AttendanceRate', 100)
    attendance_history['AttendanceRate'] = attendance_history['AttendanceRate'].astype(str)
    attendance_history = attendance_history.set_index(['StudentSchoolKey'])
    ############################
    # Discipline Action
    ############################
    student_discipline_dim_view = subset(
        student_discipline_dim_view,
        [
            'StudentSchoolKey'
        ]
    )
    discipline_action = student_discipline_dim_view
    if not student_discipline_dim_view.empty:
        discipline_action = (
            student_discipline_dim_view.groupby(
                ['StudentSchoolKey'],
                as_index=False,
                group_keys=False
            ).apply(lambda s: pd.Series({
                'ReferralsAndSuspensions': s.size
            }))
        )
    else:
        discipline_action = create_empty_data_frame(
            ['StudentSchoolKey', 'ReferralsAndSuspensions'],
            index=['StudentSchoolKey']
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
            'studentSectionAssociationReference.beginDate',
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
    student_grades_association_normalized['StudentSectionStartDateKey'] = (
        to_datetime_key(
            student_grades_association_normalized,
            'studentSectionAssociationReference.beginDate'
        )
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
        + '-' + student_grades_association_normalized['StudentSectionStartDateKey']
    )
    student_grades_association_normalized = subset(
        student_grades_association_normalized,
        [
            'StudentSectionKey',
            'gradeTypeDescriptor',
            'numericGradeEarned',
            'StudentSectionStartDateKey'
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
        student_grades_association_normalized = get_descriptor_constant(student_grades_association_normalized, 'gradeTypeDescriptor')
        student_grades_association_normalized['gradeTypeDescriptor_constantName'].str.contains('GradeType.Semester', na=False)
        student_grades_association_normalized['GradeSummary'] = (
            student_grades_association_normalized['CourseTitle']
            + ': ' + student_grades_association_normalized['numericGradeEarned'].astype(str)
        )
        student_grades_association_normalized.sort_values(
            by=[
                'StudentSchoolKey',
                'StudentSectionStartDateKey'
            ],
            inplace=True,
            ascending=False
        )
        student_grades_association_normalized = (
            student_grades_association_normalized.groupby(
                ['StudentSchoolKey'],
                as_index=False,
                group_keys=True
            ).agg(
                {'GradeSummary': '\n '.join}
            )
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
    # Student School
    ############################

    student_school_dim_view['CurrentSchoolKey'] = student_school_dim_view['SchoolKey'].astype(str)
    student_school_dim_view = subset(
        student_school_dim_view,
        [
            'StudentKey',
            'StudentSchoolKey',
            'CurrentSchoolKey'
        ]
    )
    ############################
    # Student enrollment
    ############################
    result_data_frame = pdMerge(
        left=student_school_dim_view,
        right=student_enrollment_dim_view,
        how='inner',
        leftOn=['StudentKey'],
        rightOn=['StudentKey'],
        suffixLeft=None,
        suffixRight=None
    )
    if is_data_frame_empty(result_data_frame):
        return None
    if not (attendance_history is None or attendance_history.empty):
        result_data_frame = pdMerge(
            left=result_data_frame,
            right=attendance_history,
            how='left',
            leftOn=['StudentSchoolKey'],
            rightOn=['StudentSchoolKey'],
            suffixLeft=None,
            suffixRight='attendance_history_'
        )
    replace_null(result_data_frame, 'AttendanceRate', '100')
    if not (discipline_action is None or discipline_action.empty):
        result_data_frame = pdMerge(
            left=result_data_frame,
            right=discipline_action,
            how='left',
            leftOn=['StudentSchoolKey'],
            rightOn=['StudentSchoolKey'],
            suffixLeft=None,
            suffixRight='discipline_action_'
        )
    replace_null_empty(result_data_frame, 'ReferralsAndSuspensions', 0)
    if not (discipline_action is None or discipline_action.empty):
        result_data_frame = pdMerge(
            left=result_data_frame,
            right=student_grades_association_normalized,
            how='left',
            leftOn=['StudentSchoolKey'],
            rightOn=['StudentSchoolKey'],
            suffixLeft=None,
            suffixRight='student_grades_association_normalized_'
        ).reset_index()
    replace_null(result_data_frame, 'AttendanceRate', '0')
    result_data_frame['AttendanceRate'] = result_data_frame['AttendanceRate'].astype(float)
    replace_null(result_data_frame, 'GradeSummary', '')
    result_data_frame = subset(
        result_data_frame,
        columns
    )
    return result_data_frame


def student_history_dim(school_year) -> data_frame_generation_result:
    return student_history_dim_data_frame(
        file_name="equity_StudentHistoryDim.parquet",
        columns=RESULT_COLUMNS,
        school_year=school_year
    )

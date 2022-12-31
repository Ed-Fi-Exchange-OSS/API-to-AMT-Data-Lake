# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.


from edfi_amt_data_lake.helper.data_frame_generation_result import (
    data_frame_generation_result,
)
from edfi_amt_data_lake.parquet.amt.base.all_student_school_dim.main import (
    all_student_school_dim,
)
from edfi_amt_data_lake.parquet.Common.pandasWrapper import create_parquet_file

RESULT_COLUMNS = [
    'StudentSchoolKey',
    'StudentKey',
    'SchoolKey',
    'SchoolYear',
    'StudentFirstName',
    'StudentMiddleName',
    'StudentLastName',
    'BirthDate',
    'EnrollmentDateKey',
    'GradeLevel',
    'LimitedEnglishProficiency',
    'IsHispanic',
    'Sex',
    'InternetAccessInResidence',
    'InternetAccessTypeInResidence',
    'InternetPerformance',
    'DigitalDevice',
    'DeviceAccess'
]


@create_parquet_file
def student_school_dim_data_frame(
    file_name: str,
    columns: list[str],
    school_year: int
):
    file_name = file_name

    result_data_frame = all_student_school_dim(school_year=school_year).data_frame
    if result_data_frame is None:
        return None

    result_data_frame['SchoolKey'] = result_data_frame['SchoolKey'].astype(str)
    result_data_frame['IsHispanic'] = result_data_frame['IsHispanic'].astype(int)

    result_data_frame = result_data_frame[result_data_frame['IsEnrolled'] == 1]
    return result_data_frame[
        columns
    ]


def student_school_dim(school_year) -> data_frame_generation_result:
    return student_school_dim_data_frame(
        file_name="studentSchoolDim.parquet",
        columns=RESULT_COLUMNS,
        school_year=school_year
    )

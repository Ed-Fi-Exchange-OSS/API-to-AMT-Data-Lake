# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import pandas as pd
from decouple import config

from edfi_amt_data_lake.helper.data_frame_generation_result import (
    data_frame_generation_result,
)
from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    pdMerge,
    jsonNormalize,
    renameColumns,
    create_parquet_file,
    addColumnIfNotExists,
    get_descriptor_code_value_from_uri
)

ENDPOINT_SESSIONS = "sessions"
ENDPOINT_GRADING_PERIOD_DESCRIPTORS = "gradingPeriodDescriptors"
ENDPOINT_TERM_DESCRIPTORS = "termDescriptors"
ENDPOINT_SCHOOL_YEAR_TYPES = "schoolYearTypes"

RESULT_COLUMNS = [
    "AcademicTimePeriodKey",
    "SchoolYear",
    "SchoolYearName",
    "IsCurrentSchoolYear",
    "SchoolKey",
    "SessionKey",
    "SessionName",
    "TermName",
    "GradingPeriodKey",
    "GradingPeriodName",
    "LastModifiedDate"
]


@create_parquet_file
def academic_time_period_dim_frame(
    file_name: str,
    columns: list[str],
    school_year: int
) -> pd.DataFrame:
    file_name = file_name
    sessions_content = getEndpointJson(ENDPOINT_SESSIONS, config("SILVER_DATA_LOCATION"), school_year)
    grading_periods_descriptors_content = getEndpointJson(ENDPOINT_GRADING_PERIOD_DESCRIPTORS, config("SILVER_DATA_LOCATION"), school_year)
    term_descriptors_content = getEndpointJson(ENDPOINT_TERM_DESCRIPTORS, config("SILVER_DATA_LOCATION"), school_year)
    school_year_types_content = getEndpointJson(ENDPOINT_SCHOOL_YEAR_TYPES, config("SILVER_DATA_LOCATION"), school_year)

    session_normalized = jsonNormalize(
        data=sessions_content,
        recordPath="gradingPeriods",
        meta=["sessionName", "beginDate", "endDate", "termDescriptor"],
        recordMeta=[
            "gradingPeriodReference.schoolId",
            "gradingPeriodReference.schoolYear",
            "gradingPeriodReference.gradingPeriodDescriptor",
            "gradingPeriodReference.periodSequence"
        ],
        metaPrefix=None,
        recordPrefix="session_",
        errors="ignore"
    )

    get_descriptor_code_value_from_uri(session_normalized, "termDescriptor")
    get_descriptor_code_value_from_uri(session_normalized, "session_gradingPeriodReference.gradingPeriodDescriptor")

    grading_period_descriptor_normalized = jsonNormalize(
        data=grading_periods_descriptors_content,
        recordPath=None,
        meta=[],
        recordMeta=["gradingPeriodDescriptorId", "codeValue"],
        metaPrefix=None,
        recordPrefix=None,
        errors="ignore"
    )

    term_descriptor_normalized = jsonNormalize(
        data=term_descriptors_content,
        recordPath=None,
        meta=[],
        recordMeta=["termDescriptorId", "codeValue", "description"],
        metaPrefix=None,
        recordPrefix=None,
        errors="ignore"
    )

    schoolYearTypes_normalized = jsonNormalize(
        data=school_year_types_content,
        recordPath=None,
        meta=[],
        recordMeta=["schoolYear", "currentSchoolYear", "schoolYearDescription"],
        metaPrefix=None,
        recordPrefix=None,
        errors="ignore"
    )

    session_with_term_descriptor_merged = pdMerge(
        left=session_normalized,
        right=term_descriptor_normalized,
        how="inner",
        leftOn=["termDescriptor"],
        rightOn=["codeValue"],
        suffixLeft="_session",
        suffixRight="_term_descriptor_normalized"
    )

    session_with_term_descriptor_merged_and_grading_period_descriptor_normalized = pdMerge(
        left=session_with_term_descriptor_merged,
        right=grading_period_descriptor_normalized,
        how="inner",
        leftOn=["session_gradingPeriodReference.gradingPeriodDescriptor"],
        rightOn=["codeValue"],
        suffixLeft="_session_with_term_descriptor_merged",
        suffixRight="_grading_period_descriptor_normalized"
    )

    result_data_frame = pdMerge(
        left=session_with_term_descriptor_merged_and_grading_period_descriptor_normalized,
        right=schoolYearTypes_normalized,
        how="inner",
        leftOn=["session_gradingPeriodReference.schoolYear"],
        rightOn=["schoolYear"],
        suffixLeft="_session_with_term_descriptor_merged_and_grading_period_descriptor_normalized",
        suffixRight="_schoolYearTypes_normalized"
    )

    addColumnIfNotExists(result_data_frame, "LastModifiedDate")

    result_data_frame["AcademicTimePeriodKey"] = (
        result_data_frame["session_gradingPeriodReference.schoolId"].astype(str)
        + "-" + result_data_frame["session_gradingPeriodReference.schoolYear"].astype(str)
        + "-" + result_data_frame["termDescriptorId"].astype(str)
        + "-" + result_data_frame["gradingPeriodDescriptorId"].astype(str)
        + "-" + result_data_frame["beginDate"].astype(str).str.replace("-", "")
    )

    result_data_frame["SessionKey"] = (
        result_data_frame["session_gradingPeriodReference.schoolId"].astype(str)
        + "-" + result_data_frame["session_gradingPeriodReference.schoolYear"].astype(str)
        + "-" + result_data_frame["sessionName"].astype(str)
    )

    result_data_frame["GradingPeriodKey"] = (
        result_data_frame["gradingPeriodDescriptorId"].astype(str)
        + "-" + result_data_frame["session_gradingPeriodReference.schoolId"].astype(str)
        + "-" + result_data_frame["beginDate"].astype(str).str.replace("-", "")
    )

    result_data_frame = renameColumns(result_data_frame, {
        "session_gradingPeriodReference.schoolYear": "SchoolYear",
        "schoolYearDescription": "SchoolYearName",
        "currentSchoolYear": "IsCurrentSchoolYear",
        "session_gradingPeriodReference.schoolId": "SchoolKey",
        "sessionName": "SessionName",
        "codeValue_session_with_term_descriptor_merged": "TermName",
        "codeValue_grading_period_descriptor_normalized": "GradingPeriodName",
    })

    result_data_frame = result_data_frame[columns]

    return result_data_frame[
        columns
    ]


def academic_time_period_dim(school_year) -> data_frame_generation_result:
    return academic_time_period_dim_frame(
        file_name="academicTimePeriodDim.parquet",
        columns=RESULT_COLUMNS,
        school_year=school_year
    )

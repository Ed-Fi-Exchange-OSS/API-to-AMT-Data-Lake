# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from edfi_amt_data_lake.parquet.amt.ews.student_early_warning_fact.main import (
    student_early_warning_fact,
)
from edfi_amt_data_lake.parquet.amt.ews.student_section_grade_fact.main import (
    student_section_grade_fact,
)


def ews_collection(school_year) -> None:
    student_early_warning_fact(school_year)
    student_section_grade_fact(school_year)

# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from edfi_amt_data_lake.parquet.amt.base.contact_person_dim.main import (
    contact_person_dim,
)
from edfi_amt_data_lake.parquet.amt.base.date_dim.main import date_dim
from edfi_amt_data_lake.parquet.amt.base.demographics_dim.main import demographics_dim
from edfi_amt_data_lake.parquet.amt.base.grading_period_dim.main import (
    grading_period_dim,
)
from edfi_amt_data_lake.parquet.amt.base.local_education_agency_dim.main import (
    local_education_agency_dim,
)
from edfi_amt_data_lake.parquet.amt.base.most_recent_grading_period.main import (
    most_recent_grading_period,
)
from edfi_amt_data_lake.parquet.amt.base.school_dim.main import school_dim
from edfi_amt_data_lake.parquet.amt.base.section_dim.main import section_dim
from edfi_amt_data_lake.parquet.amt.base.student_local_education_agency_demographics_bridge.main import (
    student_local_education_agency_demographics_bridge,
)
from edfi_amt_data_lake.parquet.amt.base.student_program_dim.main import (
    student_program_dim,
)


def base_collection(school_year) -> None:
    contact_person_dim(school_year)
    date_dim(school_year)
    demographics_dim(school_year)
    grading_period_dim(school_year)
    local_education_agency_dim(school_year)
    most_recent_grading_period(school_year)
    school_dim(school_year)
    student_program_dim(school_year)
    section_dim(school_year)
    student_local_education_agency_demographics_bridge(school_year)

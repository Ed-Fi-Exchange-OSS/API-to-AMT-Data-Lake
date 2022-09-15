# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.
from edfi_amt_data_lake.parquet.amt.equity.feeder_school_dim.main import (
    feeder_school_dim,
)
from edfi_amt_data_lake.parquet.amt.equity.student_discipline_action_dim.main import (
    student_discipline_action_dim,
)
from edfi_amt_data_lake.parquet.amt.equity.StudentSchoolFoodServiceProgramDim.script import (
    studentSchoolFoodServiceProgramDim,
)


def equity_collection(school_year) -> None:
    student_discipline_action_dim(school_year)
    feeder_school_dim(school_year)
    studentSchoolFoodServiceProgramDim(school_year)

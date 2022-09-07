# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from edfi_amt_data_lake.parquet.SchoolDim.script import schoolDim
from edfi_amt_data_lake.parquet.AssessmentFact.script import AssessmentFact
from edfi_amt_data_lake.parquet.StudentEarlyWarningFactDim.script import StudentEarlyWarningFactDim

def run() -> None:
    schoolDim()
    AssessmentFact()
    StudentEarlyWarningFactDim()
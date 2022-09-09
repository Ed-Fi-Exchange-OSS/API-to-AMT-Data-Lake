# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from edfi_amt_data_lake.parquet.amt.ews.StudentEarlyWarningFactDim.script import studentEarlyWarningFactDim
from edfi_amt_data_lake.parquet.amt.ews.StudentSectionGradeFact.script import StudentSectionGradeFact

def ews_collection(school_year) -> None:
    studentEarlyWarningFactDim(school_year)
    StudentSectionGradeFact(school_year)
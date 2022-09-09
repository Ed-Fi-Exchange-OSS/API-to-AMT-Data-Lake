# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from edfi_amt_data_lake.parquet.amt.base.SchoolDim.script import schoolDim

def base_collection(school_year) -> None:
    schoolDim(school_year)

# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from edfi_amt_data_lake.parquet.amt.rls.rls_student_data_authorization.main import (
    rls_student_data_authorization,
)
from edfi_amt_data_lake.parquet.amt.rls.rls_user_dim.main import rls_user_dim
from edfi_amt_data_lake.parquet.amt.rls.rls_user_student_data_authorization.main import (
    rls_user_student_data_authorization,
)
from edfi_amt_data_lake.parquet.amt.rls.rls_staff_classification_descriptor_scope_list.main import (
    rls_staff_classification_descriptor_scope_list,
)

def rls_collection(school_year) -> None:
    rls_student_data_authorization(school_year)
    rls_user_dim(school_year)
    rls_user_student_data_authorization(school_year)
    rls_staff_classification_descriptor_scope_list(school_year)

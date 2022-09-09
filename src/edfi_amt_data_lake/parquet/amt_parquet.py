# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from edfi_amt_data_lake.parquet.amt.asmt.asmt_collection import asmt_collection
from edfi_amt_data_lake.parquet.amt.base.base_collection import base_collection
from edfi_amt_data_lake.parquet.amt.chrab.chrab_collection import chrab_collection
from edfi_amt_data_lake.parquet.amt.engage.engage_collection import engage_collection
from edfi_amt_data_lake.parquet.amt.epp.epp_collection import epp_collection
from edfi_amt_data_lake.parquet.amt.equity.equity_collection import equity_collection
from edfi_amt_data_lake.parquet.amt.ews.ews_collection import ews_collection
from edfi_amt_data_lake.parquet.amt.qews.qews_collection import qews_collection
from edfi_amt_data_lake.parquet.amt.rls.rls_collection import rls_collection

def generate_amt_parquet(school_year="") -> None:
    asmt_collection(school_year)
    base_collection(school_year)
    chrab_collection(school_year)
    engage_collection(school_year)
    epp_collection(school_year)
    equity_collection(school_year)
    ews_collection(school_year)
    qews_collection(school_year)
    rls_collection(school_year)
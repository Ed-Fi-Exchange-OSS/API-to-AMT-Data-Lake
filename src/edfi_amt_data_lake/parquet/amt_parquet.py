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

def generate_amt_parquet() -> None:
    asmt_collection()
    base_collection()
    chrab_collection()
    engage_collection()
    epp_collection()
    equity_collection()
    ews_collection()
    qews_collection()
    rls_collection()
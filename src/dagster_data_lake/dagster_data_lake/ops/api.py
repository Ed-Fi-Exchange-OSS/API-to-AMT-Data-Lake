# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from dagster import asset, materialize
from edfi_amt_data_lake.api import get_data

@asset
def api():
    return get_data()

if __name__ == "__main__":
    materialize([api])

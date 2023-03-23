# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from dagster import job
from dagster_config.ops.api import generate_parquet, get_api_data


@job
def pipe_api_job():
    generate_parquet(get_api_data())

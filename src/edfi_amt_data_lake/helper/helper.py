# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import json
import os
from decouple import config
from dagster.utils import file_relative_path

from edfi_amt_data_lake.helper.base import JSONFile

# List of endpoints from API
def get_endpoint() -> list:
    with open(file_relative_path(
        __file__, './endpoint/endpoint.json'), "r") as file:
        data = json.load(file)
    return data

# Create a function to save JSON into a file in the json directory.
def save_file(json_file: JSONFile, json_file_sufix, data) -> None:
    if data:
        jsonsLocation = config('SILVER_DATA_LOCATION')
        path = f"{jsonsLocation}{json_file.directory}"
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)

        with open(f"{jsonsLocation}/{json_file.directory}/{json_file.name}_{json_file_sufix}.json", "w") as file:
            json.dump(data, file, indent=4)
        print(f"{json_file.name}({json_file_sufix}) Saved!")

# Create a function to get endpoint url.
def get_url(endpoint: str) -> str:
    return f"{config('API_URL')}/{config('PREX_DATA_V')}/{endpoint}"

# Get headers for API call.
def get_headers(token) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

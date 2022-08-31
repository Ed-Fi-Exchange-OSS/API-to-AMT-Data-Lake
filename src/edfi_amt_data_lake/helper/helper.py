# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import json
import os
from decouple import config
from dagster.utils import file_relative_path

from edfi_amt_data_lake.helper.base import JSONFile
API_MODE = config("API_MODE")
SCHOOL_YEAR = config("SCHOOL_YEAR")

# if API_MODE is YearSpecific, returns the SchoolYear list
def get_school_year() -> list:
    school_year = f"{SCHOOL_YEAR}" if API_MODE == "YearSpecific" else ""
    return school_year.split(",")

# List of endpoints from API
def get_endpoint() -> list:
    with open(file_relative_path(
        __file__, './endpoint/endpoint.json'), "r") as file:
        data = json.load(file)
    return data

# Create a function to save JSON into a file in the json directory.
def save_file(json_file: JSONFile, json_file_sufix, data,school_year="") -> None:
    school_year_path = f"/{school_year}/" if school_year else ""
    if data:
        jsonsLocation = config('SILVER_DATA_LOCATION')
        path = f"{jsonsLocation}{school_year_path}{json_file.directory}"
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)

        with open(f"{jsonsLocation}{school_year_path}{json_file.directory}/{json_file.name}_{json_file_sufix}.json", "w") as file:
            json.dump(data, file, indent=4)
        print(f"{json_file.name}({json_file_sufix}) Saved!")

# Create a function to get endpoint url.
def get_url(endpoint: str, school_year="", is_deletes_endpoint=False) -> str:
    deletes = "/deletes" if is_deletes_endpoint else ""
    school_year_url=f"/{school_year}" if school_year else ""
    return f"{config('API_URL')}/{config('PREX_DATA_V')}{school_year_url}/{endpoint}{deletes}"

# Get headers for API call.
def get_headers(token) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

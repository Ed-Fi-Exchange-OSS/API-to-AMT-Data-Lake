# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import json
import os

from dagster.utils import file_relative_path
from decouple import config

from edfi_amt_data_lake.helper.base import JSONFile

API_MODE = config("API_MODE")
SCHOOL_YEAR = config("SCHOOL_YEAR")


# if API_MODE is YearSpecific, returns the SchoolYear list
def get_school_year() -> list:
    school_year = f"{SCHOOL_YEAR}" if API_MODE == "YearSpecific" else ""
    school_list = school_year.split(",") if school_year else [""]
    return school_list


# List of endpoints from API
def get_endpoint() -> list:
    with open(file_relative_path(__file__, './endpoint/endpoint.json'), "r") as file:
        data = json.load(file)
    return data


# Create a function to save JSON into a file in the json directory.
def save_file(json_file: JSONFile, json_file_sufix: any, data: any, school_year: str) -> None:
    school_year_path = f"/{school_year}/" if school_year else ""
    if data:
        json_location = config('SILVER_DATA_LOCATION')
        path = f"{json_location}{school_year_path}{json_file.directory}"
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)

        with open(f"{json_location}{school_year_path}{json_file.directory}/{json_file.name}_{json_file_sufix}.json", "w") as file:
            json.dump(data, file, indent=4)
            file_size = os.path.getsize(f"{json_location}{school_year_path}{json_file.directory}/{json_file.name}_{json_file_sufix}.json") / 1000000
            print(f"File {json_file.name}({json_file_sufix}) saved with {file_size} MB")


# Create a function to get endpoint url.
def get_url(endpoint: str, school_year: str, is_deletes_endpoint: bool = False) -> str:
    deletes = "/deletes" if is_deletes_endpoint else ""
    school_year_url = f"/{school_year}" if school_year else ""
    return f"{config('API_URL')}/{config('PREX_DATA_V')}{school_year_url}/{endpoint}{deletes}"


# Get headers for API call.
def get_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }


def get_descriptor_mapping_config() -> list:
    with open(file_relative_path(__file__, './descriptor_map/descriptor_map.json'), "r") as file:
        data = json.load(file)
    return data

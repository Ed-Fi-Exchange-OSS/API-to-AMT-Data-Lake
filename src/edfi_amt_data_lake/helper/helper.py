# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import json
import os

from dagster import get_dagster_logger
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
    parquet_logger = get_dagster_logger()
    if data:
        json_location = get_path(config('SILVER_DATA_LOCATION'), school_year)
        path = os.path.join(json_location, json_file.directory)
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
        file_path = os.path.join(path, f'{json_file.name}_{json_file_sufix}.json')
        with open(file_path, "w") as file:
            json.dump(data, file, indent=4)
            file_size = os.path.getsize(file_path) / 1000000
            parquet_logger.info(f"File {json_file.name}({json_file_sufix}) saved with {file_size} MB")


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


def get_path(path: str, school_year: str):
    school_year_path = f"{school_year}/" if school_year else ""
    destination_folder = os.path.join(path, school_year_path) if school_year_path == '' else path
    return destination_folder


def delete_files_by_extension(path: str, extension_list: list[str]) -> None:
    parquet_logger = get_dagster_logger()
    parquet_logger.info('Delete parquet files')
    files_to_delete = os.listdir(path)
    count_deleted_files = 0
    for file in files_to_delete:
        for extension in extension_list:
            if file.endswith(f"{extension}"):
                file_to_remove = os.path.join(path, file)
                os.remove(file_to_remove)
                parquet_logger.debug(f'Deleted file: {file_to_remove}')
                count_deleted_files += 1
    parquet_logger.info(f'Deleted files: {count_deleted_files}')


def clean_parquet_folder(school_year: str) -> None:
    path = config('PARQUET_FILES_LOCATION')
    destination_path = get_path(path, school_year)
    delete_files_by_extension(
        path=destination_path,
        extension_list=['parquet']
    )

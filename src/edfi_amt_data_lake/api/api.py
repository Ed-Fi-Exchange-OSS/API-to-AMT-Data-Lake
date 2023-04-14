# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import traceback
from typing import Any

import requests
from dagster import get_dagster_logger
from decouple import config

from edfi_amt_data_lake.helper.base import PATH, JSONFile
from edfi_amt_data_lake.helper.changeVersionValues import ChangeVersionValues
from edfi_amt_data_lake.helper.data_model_values import data_model_values
from edfi_amt_data_lake.helper.helper import (
    get_endpoint,
    get_headers,
    get_url,
    save_file,
)
from edfi_amt_data_lake.helper.token import get_token

API_LIMIT = config("API_LIMIT", cast=int)
LIMIT = API_LIMIT if API_LIMIT else 500
SUPPORTED_VERSION = ["3.3", "4.0"]
DATA_MODEL = {"edfi": "Ed-Fi", "tpdm": "TPDM"}


# Get the newest and oldest change version values
def _get_change_version_values(school_year: Any) -> ChangeVersionValues:
    school_year_path = f"{school_year}/" if school_year else ""
    path_filename = f"{config('CHANGE_VERSION_FILEPATH')}/{school_year_path}changeVersion.txt"
    with open(path_filename, "r") as outfile:
        values = outfile.readlines()
        if len(values) == 2:
            pre_version = values[0].rstrip('\n')
            pos_version = values[1].rstrip('\n')
            return ChangeVersionValues(pre_version, pos_version)
    return ChangeVersionValues('0', '0')


def _get_api_data_models() -> list:
    logger = get_dagster_logger()
    url = f"{config('API_URL')}"
    result = []
    try:
        verify_cert = config('REQUESTS_CERT_VERIFICATION', default=True, cast=bool)
        response = requests.get(url, verify=verify_cert)
        if response.ok:
            response_data = response.json()
            result = response_data["dataModels"]
        else:
            logger.error(f"Get API Data Response: {response.status_code} - {response.reason}.")
    except Exception as ex:
        logger.error(f"An unhandled exception occured: {ex}, Traceback: {traceback.format_exc()}")
    return result


def _get_data_model_by_name(name: str) -> data_model_values:
    data_model_list = _get_api_data_models()
    for data_model in data_model_list:
        if data_model["name"].lower() == name.lower():
            return data_model_values(
                data_model.get("name",),
                data_model.get("version"),
                data_model.get("informationalVersion")
            )
    return data_model_values("", "", "")


def validate_supported_api() -> bool:
    ed_fi_model = _get_data_model_by_name(DATA_MODEL["edfi"])
    if ed_fi_model.version:
        for version in SUPPORTED_VERSION:
            if ed_fi_model.version.startswith(version):
                return True
    return False


def is_tpdm_supported() -> bool:
    if _get_data_model_by_name(DATA_MODEL["tpdm"]).name:
        return True
    return False


# Get a response from the Ed-Fi API
def _api_call(url: str, token: str, version: ChangeVersionValues) -> list:
    logger = get_dagster_logger()
    offset = 0
    result: list[Any]
    result = []
    loop = True
    headers = get_headers(token)
    change_version_parameters = (
        f"&minChangeVersion={version.oldestChangeVersion}&maxChangeVersion={version.newestChangeVersion}"
        if not config('DISABLE_CHANGE_VERSION', default=True, cast=bool)
        else ""
    )
    try:
        while loop:
            endpoint = (
                f"{url}?limit={LIMIT}&offset={offset}{change_version_parameters}"
            )
            verify_cert = config('REQUESTS_CERT_VERIFICATION', default=True, cast=bool)
            response = requests.get(endpoint, headers=headers, verify=verify_cert)
            response_data = ''
            if response.ok:
                response_data = response.json()
                result.extend(response_data)
                offset += LIMIT
            if len(response_data) == 0:
                loop = False
    except BaseException as err:
        logger.error(f"Unexpected {err=}, {type(err)=}")
    return result


# Get JSON from API endpoint and save to file
def api_async(school_year: Any = None) -> None:
    import os
    from multiprocessing import Pool
    token = get_token()
    version = _get_change_version_values(school_year)
    os_cpu = config("OS_CPU", cast=int) if config("OS_CPU") else os.cpu_count()
    with Pool(processes=os_cpu) as pool:
        for endpoint in get_endpoint():
            url = get_url(endpoint[PATH], f"{school_year}")
            url_name = JSONFile(url.split("/")[-1])
            data_async = pool.apply_async(_api_call, args=(url, token, version))
            save_file(url_name, version.newestChangeVersion, data_async.get(), f"{school_year}")

            # Deletes endpoint
            deletes_endpoint = get_url(endpoint[PATH], f"{school_year}", True)
            data_deletes_response_async = pool.apply_async(_api_call, args=(deletes_endpoint, token, version))
            save_file(url_name, f"deletes_{version.newestChangeVersion}", data_deletes_response_async.get(), f"{school_year}")
    return None


if __name__ == "__main__":
    api_async()

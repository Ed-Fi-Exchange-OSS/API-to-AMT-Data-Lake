# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import os
from pathlib import Path

import requests
from decouple import config

from edfi_amt_data_lake.helper.changeVersionValues import ChangeVersionValues
from edfi_amt_data_lake.helper.token import get_token
from edfi_amt_data_lake.helper.utils import delete_path_content


def create_file_if_not_exists(filepath, path) -> None:
    os.makedirs(path, exist_ok=True)

    filePath = Path(filepath)
    filePath.touch(exist_ok=True)


def get_change_version_values_from_file(file) -> ChangeVersionValues:
    with open(file, "r") as outfile:
        values = outfile.readlines()
        if len(values) == 2:
            return ChangeVersionValues(values[0].replace('\n', ''), values[1])

    return ChangeVersionValues('0', '0')


def get_change_version_values_from_api(school_year="") -> ChangeVersionValues:
    token = get_token()
    verify_cert = config('REQUESTS_CERT_VERIFICATION', default=True, cast=bool)
    school_year_url = f"{school_year}/" if school_year else ""
    url = f"{config('API_URL')}{config('AVAILABLE_CHANGE_VERSIONS').format(school_year_url)}"
    headers = {"Authorization": "Bearer " + token}
    response = requests.get(url, headers=headers, verify=verify_cert)
    changeVersionValues = ChangeVersionValues("", "")
    if response.ok:
        response_json = response.json()

        oldestChangeVersion = str(response_json.get("oldestChangeVersion", response_json.get("OldestChangeVersion", "")))
        newestChangeVersion = str(response_json.get("newestChangeVersion", response_json.get("NewestChangeVersion", "")))
        changeVersionValues = ChangeVersionValues(oldestChangeVersion, newestChangeVersion)

    return changeVersionValues


def _update_change_version_file(pathfilename: str, oldestChangeVersion: str, newestChangeVersion: str) -> None:
    with open(pathfilename, "w") as outfile:
        fileLines = [f"{oldestChangeVersion}\n", newestChangeVersion]
        outfile.writelines(fileLines)


def get_change_version_updated(school_year) -> bool:
    school_year_path = f"{school_year}/" if school_year else ""
    path = config("CHANGE_VERSION_FILEPATH") + f"/{school_year_path}"
    pathfilename = f"{path}changeVersion.txt"

    create_file_if_not_exists(pathfilename, path)

    # Get the Change Version values from the API.
    changeVersionFromAPI = get_change_version_values_from_api(school_year)

    disable_change_version = config("DISABLE_CHANGE_VERSION", default=True, cast=bool)
    if disable_change_version:
        delete_path_content(config("CHANGE_VERSION_FILEPATH"))
        create_file_if_not_exists(pathfilename, path)
        _update_change_version_file(pathfilename, "0", changeVersionFromAPI.newestChangeVersion)
        return True

    # Read the Change Version values from the file if they exists.
    changeVersionFromFile = get_change_version_values_from_file(pathfilename)

    oldestChangeVersion = ''
    newestChangeVersion = ''

    if changeVersionFromFile.newestChangeVersion == 0:
        # First Scenario: Fist time we are saving these values locally.
        oldestChangeVersion = changeVersionFromAPI.oldestChangeVersion
        newestChangeVersion = changeVersionFromAPI.newestChangeVersion
    elif changeVersionFromFile.newestChangeVersion == changeVersionFromAPI.newestChangeVersion:
        # Second Scenario: newestChangeVersion from file and api are the same. No need to call the api.
        return False
    else:
        # Third Scenario: Updating the file is required.
        oldestChangeVersion = changeVersionFromFile.newestChangeVersion
        newestChangeVersion = changeVersionFromAPI.newestChangeVersion

    _update_change_version_file(pathfilename, oldestChangeVersion, newestChangeVersion)

    return True

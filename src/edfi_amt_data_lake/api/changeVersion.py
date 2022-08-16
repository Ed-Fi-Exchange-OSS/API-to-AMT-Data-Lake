# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import requests
import os
from pathlib import Path
from decouple import config

from edfi_amt_data_lake.helper.token import get_token
from edfi_amt_data_lake.helper.changeVersionValues import ChangeVersionValues

def create_file_if_not_exists(filepath, path) -> None:
    os.makedirs(path, exist_ok=True)
    
    filePath = Path(filepath)
    filePath.touch(exist_ok = True)

def get_change_version_values_from_file(file) -> ChangeVersionValues:
    with open(file, "r") as outfile:
        values = outfile.readlines()
        if len(values) == 2:
            return ChangeVersionValues(values[0].replace('\n', ''), values[1])
        
    return ChangeVersionValues('0', '0')

def get_change_version_values_from_api() -> ChangeVersionValues:
    token = get_token()
    url = f"{config('API_URL')}{config('AVAILABLE_CHANGE_VERSIONS')}"
    headers = {"Authorization": "Bearer " + token}
    response =  requests.get(url, headers=headers)

    if response.ok:
        response_json = response.json()
        
        oldestChangeVersion = str(response_json["OldestChangeVersion"])
        newestChangeVersion = str(response_json["NewestChangeVersion"])
        changeVersionValues = ChangeVersionValues(oldestChangeVersion, newestChangeVersion)

    return changeVersionValues

def get_change_version_updated() -> bool:
    path = config("CHANGE_VERSION_FILEPATH") + "API_TO_AMT/"
    filename = config("CHANGE_VERSION_FILENAME")
    pathfilename = f"{path}{filename}"
    
    create_file_if_not_exists(pathfilename, path)

    # Read the Change Version values from the file if they exists.
    changeVersionFromFile = get_change_version_values_from_file(pathfilename)

    # Get the Change Version values from the API.
    changeVersionFromAPI =  get_change_version_values_from_api()

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

    with open(pathfilename, "w") as outfile:
        fileLines = [f"{oldestChangeVersion}\n",newestChangeVersion]
        outfile.writelines(fileLines)

    return True

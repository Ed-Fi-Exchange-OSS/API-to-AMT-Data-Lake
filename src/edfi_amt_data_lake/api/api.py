# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import requests
from decouple import config

from edfi_amt_data_lake.helper.token import get_token
from edfi_amt_data_lake.helper.base import PATH, JSONFile
from edfi_amt_data_lake.helper.helper import save_file, get_endpoint, get_url
from edfi_amt_data_lake.helper.changeVersionValues import ChangeVersionValues

API_LIMIT = config("API_LIMIT", cast=int)
LIMIT = API_LIMIT if API_LIMIT else 500

def get_ChangeVersionValues() -> ChangeVersionValues:
    path = config("CHANGE_VERSION_FILEPATH") + "API_TO_AMT/"
    filename = config("CHANGE_VERSION_FILENAME")
    pathfilename = f"{path}{filename}"

    with open(pathfilename, "r") as outfile:
        values = outfile.readlines()
        if len(values) == 2:
            return ChangeVersionValues(values[0].replace('\n', ''), values[1])
    
    return ChangeVersionValues('0', '0')

# Get a response from the Ed-Fi API
def _call(url, token, changeVersionValues) -> list:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    offset = 0; result = []
    try:
        while True:
            endpoint = f"{url}?limit={LIMIT}&offset={offset}&minChangeVersion={changeVersionValues.oldestChangeVersion}&maxChangeVersion={changeVersionValues.newestChangeVersion}"
            response =  requests.get(endpoint, headers=headers)
            if response.ok:
                data = response.json()
                result.extend(data)
                offset += LIMIT
            raise Exception("No data found") if len(data) == 0 else None
    except Exception as _:
        None
    return result

# Get JSON from API endpoint and save to file
def get_all() -> None:
    toke = get_token()
    changeVersionValues = get_ChangeVersionValues()
    for endpoint in get_endpoint():
        url = get_url(endpoint[PATH])
        data = _call(url, toke, changeVersionValues)
        endpoint_name = url.split("/")[-1]
        save_file(JSONFile(endpoint_name), changeVersionValues.newestChangeVersion, data)
    return None

if __name__ == "__main__":
    get_all()

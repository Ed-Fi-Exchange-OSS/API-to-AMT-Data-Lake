# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import json
import requests
from decouple import config

from edfi_amt_data_lake.helper.token import get_token
from edfi_amt_data_lake.helper.file import JSONFile
from edfi_amt_data_lake.helper.helper import save_json_response

# Get a response from the Ed-Fi API
def _api_get(url, token):
    headers = {"Authorization": "Bearer " + token}
    response = requests.get(url, headers=headers)
    return response.json()

# Call the Ed-Fi API
def _call_api(url):
    token = get_token()
    return _api_get(url, token)

# List of endpoints from API
def _get_endpoint():
    with open(f"./endpoint/endpoint.json") as endpoint_file:
        data = json.load(endpoint_file)
    return data

# Get JSON from API endpoint and save to file
def get_data():
    json_len = len(_get_endpoint())
    for i in range(0, json_len):
        endpoint = _get_endpoint()[i]["endpoint"]
        endpoint_name = endpoint.split("/")[-1]
        file: JSONFile = JSONFile(endpoint_name)
        url = f"{config('API_URL')}/{endpoint}"
        data = _call_api(url)
        save_json_response(file, data)
    return None

if __name__ == "__main__":
    get_data()

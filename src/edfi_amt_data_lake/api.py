# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import json
import requests
from decouple import config


from edfi_amt_data_lake.helper.token import get_token
from edfi_amt_data_lake.helper.file import ENDPOINT, JSONFile
from edfi_amt_data_lake.helper.helper import save_response

from dagster.utils import file_relative_path

# Get a response from the Ed-Fi API
def _api_get(url, token) -> list:
    headers = {"Authorization": "Bearer " + token}

    limit = 20
    endpoint = (
                f"{url}" f"?limit={limit}"
            )
    offset = 0

    json_response = []
    while True:
        endpoint_to_call = f"{endpoint}&offset={offset}"
        response =  requests.get(endpoint_to_call, headers=headers)

        data =response.json()
        json_response.extend(data)

        if offset==100 or not json_response:
            # retrieved all data from api
            break
        else:
            # move onto next page
            offset = offset + limit
        print("continue")
    print("end ")

    return json.dumps(json_response)

# Call the Ed-Fi API
def _call_api(url) -> list:
    token = get_token()


    return _api_get(url, token)

# List of endpoints from API
def _get_endpoint() -> list:
    with open(file_relative_path(__file__, './endpoint/endpoint.json'), "r") as file:
        data = json.load(file)
    return data

# Get JSON from API endpoint and save to file
def get_data() -> None:
    data = _get_endpoint()
    data_len = len(data)
    for i in range(0, data_len):
        path = data[i][ENDPOINT]
        path_name = path.split("/")[-1]
        json_file = JSONFile(path_name)
        url = config("API_URL") + path
        result = _call_api(url)
        save_response(json_file, result)
    return None

if __name__ == "__main__":
    get_data()

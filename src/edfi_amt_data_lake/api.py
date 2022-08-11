# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import json
import requests
import time
from decouple import config
from datetime import datetime


from edfi_amt_data_lake.helper.token import get_token
from edfi_amt_data_lake.helper.file import ENDPOINT, JSONFile
from edfi_amt_data_lake.helper.helper import save_response

from dagster.utils import file_relative_path

# Get a response from the Ed-Fi API
def _api_get(url, token) -> list:
    headers = {"Authorization": "Bearer " + token}

    limit = int(config('API_LIMIT'))
    endpoint = (
                f"{url}" f"?limit={limit}"
            )
    offset = 0
    start = time.time()
    json_response = []
    data = None
    try:
        while True:
            
            endpoint_to_call = f"{endpoint}&offset={offset}"
            response =  requests.get(endpoint_to_call, headers=headers)
            if response.ok:
                data =response.json()
                json_response.extend(data)
            else:
                data = None
                break
            if not data:
                # retrieved all data from api
                break
            else:
                # move onto next page
                offset = offset + limit
    except Exception as e:
        print(f"***************\nEndpoint: {endpoint}\n")
        print(f"Offset: {offset}\n")
        print(f"Message: {e}\n***************\n")
    end = time.time()
    #elapsed_time = end - start

    return json_response

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

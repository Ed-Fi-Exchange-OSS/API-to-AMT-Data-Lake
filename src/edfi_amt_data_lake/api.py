# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import requests
from decouple import config

from edfi_amt_data_lake.helper.token import get_token
from edfi_amt_data_lake.helper.file import JSONFile
from edfi_amt_data_lake.helper.helper import save_json_response

# Get a response from the Ed-Fi API
def api_get(url, token):
    headers = {"Authorization": "Bearer " + token}
    response = requests.get(url, headers=headers)
    return response.json()

# Call the Ed-Fi API
def call_api(url):
    token = get_token()
    return api_get(url, token)

# Get the list of schools from the Ed-Fi API
# This is a example of how to use the API to get a list of schools
def get_schools():
    url = config("API_URL") + '/schools'
    file: JSONFile = JSONFile('schools')
    data = call_api(url)
    save_json_response(file, data)
    return data

if __name__ == "__main__":
    get_schools()

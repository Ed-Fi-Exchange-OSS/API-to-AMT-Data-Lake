# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import requests
from decouple import config

from edfi_amt_data_lake.helper.token import get_token
from edfi_amt_data_lake.helper.base import PATH, JSONFile
from edfi_amt_data_lake.helper.helper import save_file, get_endpoint, get_url

LIMIT = int(config("API_LIMIT"))

# Get a response from the Ed-Fi API
def _call(url, token) -> list:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    offset = 0; result = []
    try:
        while True:
            endpoint = f"{url}?limit={LIMIT}&offset={offset}"
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
    for endpoint in get_endpoint():
        url = get_url(endpoint[PATH])
        data = _call(url, toke)
        endpoint_name = url.split("/")[-1]
        save_file(JSONFile(endpoint_name), data)
    return None

if __name__ == "__main__":
    get_all()

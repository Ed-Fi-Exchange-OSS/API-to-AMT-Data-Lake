# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import base64

import requests
from decouple import config


# Get the API key from the environment variables
def get_token() -> str:
    # Get the token from the Ed-Fi API
    api_user = config('API_KEY')
    api_password = config('API_SECRET')
    api_url_token = f"{config('API_URL')}/{config('PREX_TOKEN')}"

    credential = ":".join((api_user, api_password))
    credential_encoded = base64.b64encode(credential.encode("utf-8"))
    access_headers = {"Authorization": b"Basic " + credential_encoded}
    access_params = {"grant_type": "client_credentials"}

    response = requests.post(api_url_token, headers=access_headers, data=access_params)

    if response.status_code == 200:
        return response.json()["access_token"]

    else:
        return None

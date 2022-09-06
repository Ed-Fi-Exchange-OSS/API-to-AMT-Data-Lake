# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import requests
from decouple import config

from edfi_amt_data_lake.helper.token import get_token
from edfi_amt_data_lake.helper.base import PATH, JSONFile
from edfi_amt_data_lake.helper.helper import save_file, get_endpoint, get_url, get_headers
from edfi_amt_data_lake.helper.changeVersionValues import ChangeVersionValues

API_LIMIT = config("API_LIMIT", cast=int)
LIMIT = API_LIMIT if API_LIMIT else 500

def _get_change_version_values(school_year="") -> ChangeVersionValues:
    school_year_path = f"{school_year}/"  if school_year else ""
    path_filename = f"{config('CHANGE_VERSION_FILEPATH')}/API_TO_AMT/{school_year_path}{config('CHANGE_VERSION_FILENAME')}"
    with open(path_filename, "r") as outfile:
        values = outfile.readlines()
        if len(values) == 2:
            pre_version = values[0].rstrip('\n')
            pos_version = values[1].rstrip('\n')
            return ChangeVersionValues(pre_version, pos_version)
    return ChangeVersionValues('0', '0')

# Get a response from the Ed-Fi API
def _call(url, token, changeVersionValues) -> list:
    headers = get_headers(token)
    offset = 0; result = []
    try:
        continue_loop = True
        while continue_loop:
            endpoint = f"{url}?limit={LIMIT}&offset={offset}&minChangeVersion={changeVersionValues.oldestChangeVersion}&maxChangeVersion={changeVersionValues.newestChangeVersion}"
            response =  requests.get(endpoint, headers=headers)
            if response.ok:
                data = response.json()
                result.extend(data)
                offset += LIMIT
            if len(data) == 0:
                continue_loop = False
    except Exception as _:
        print (_)
    return result

# Get JSON from API endpoint and save to file
def get_all(school_year="") -> None:
    import os
    from multiprocessing import Pool
    toke = get_token()
    changeVersionValues = _get_change_version_values(school_year)
    os_cpu = config("OS_CPU", cast=int) if config("OS_CPU") else os.cpu_count()
    with Pool(processes=os_cpu) as pool:
        for endpoint in get_endpoint():
            url = get_url(endpoint[PATH],school_year)
            data = pool.apply_async(_call, args=(url, toke, changeVersionValues))
            result = data.get()
            endpoint_name = url.split("/")[-1]
            save_file(JSONFile(endpoint_name), changeVersionValues.newestChangeVersion, result,school_year)
            #Deletes endpoint
            url_deletes = get_url(endpoint[PATH],school_year,True)
            data_deletes = _call(url_deletes, toke, changeVersionValues)
            save_file(JSONFile(endpoint_name), f"deletes_{changeVersionValues.newestChangeVersion}", data_deletes,school_year)
    return None

if __name__ == "__main__":
    get_all()
# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import json
import os

from edfi_amt_data_lake.helper.file import JSONFile


# Create the directory if it doesn't exist.
def create_directory(json_file: JSONFile) -> None:
    directory = f"./jsons/{json_file.directory}"
    if not os.path.exists(json_file.directory):
        os.makedirs(directory, exist_ok=True)
    return None

# Create a function to save JSON into a file in the json directory.
def save_response(json_file: JSONFile, data) -> None:
    create_directory(json_file)
    with open(f"./jsons/{json_file.directory}/{json_file.name}.json", "w") as outfile:
        json.dump(data, outfile, indent=4)
    print(f"Saved {json_file.name}")
    return None

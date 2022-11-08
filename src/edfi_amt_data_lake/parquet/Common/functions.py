# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import json
import os


def getEndpointJson(endpoint: str, rawDataLocation: str, school_year: str) -> str:
    school_year_path = f"/{school_year}/" if school_year else ""
    endpointFilePath = f"{rawDataLocation}{school_year_path}{endpoint}"
    if os.path.isdir(endpointFilePath):
        jsonFiles = [pos_json for pos_json in os.listdir(endpointFilePath) if pos_json.endswith('.json')]

        if len(jsonFiles):
            with open(f"{endpointFilePath}/{jsonFiles[0]}", "r") as jsonFile:
                jsonContent = json.loads(jsonFile.read())
            return jsonContent
        else:
            return ''
    else:
        return ''

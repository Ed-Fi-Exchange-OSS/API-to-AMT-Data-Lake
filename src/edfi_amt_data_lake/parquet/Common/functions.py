# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import json,os

def getEndpointJson(endpoint = str) -> str:
    rawDataLocation = "C:\\GAP\\EdFi\\BIA-1148\\API-to-AMT-Data-Lake\\src\\jsons\\"
    endpointFilePath = f"{rawDataLocation}{endpoint}"
    jsonFiles = [pos_json for pos_json in os.listdir(endpointFilePath) if pos_json.endswith('.json')]

    if len(jsonFiles):
        with open(f"{endpointFilePath}\\{jsonFiles[0]}", "r") as jsonFile:
            jsonContent = json.loads(jsonFile.read())
        return jsonContent
    else:
        raise Exception("Not file found for this endpoint")
# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

# Constants for endpoint.json
ENDPOINT = 'endpoint'
TABLE_NAME = 'table_name'

class JSONFile:
    def __init__(self, name):
        self.name = name
        self.directory = name

# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.


def delete_path_content(path) -> None:
    import shutil
    import time
    shutil.rmtree(path, ignore_errors=True, onerror=None)
    time.sleep(1)

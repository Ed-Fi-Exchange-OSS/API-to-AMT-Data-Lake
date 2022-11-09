# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.
import pandas as pd


class data_frame_generation_result:
    def __init__(self, successful:bool=True, data_frame:pd.DataFrame=None, columns:list[str]=None, exception:Exception = None):
        self.successful = successful
        self.data_frame = data_frame
        self.total_rows = 0
        self.exception = exception
        if not(data_frame is None) and not(data_frame.empty) and successful:
            self.total_rows = len(data_frame.index)
        else:
            self.data_frame = pd.DataFrame(columns=columns)
            
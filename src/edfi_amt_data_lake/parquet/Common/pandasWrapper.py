# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import pandas as pd

def pdMerge(left = pd.DataFrame, right = pd.DataFrame, how = str, leftOn = [str], rigthOn = [str], suffixLeft = '_x', suffixRight = '_y') -> pd.DataFrame:
    return pd.merge(
        left,
        right,
        how=how,
        left_on=leftOn,
        right_on=rigthOn,
        suffixes=(suffixLeft, suffixRight)
    )

# Use this method to review a dataframe content
def toCsv(csvContent = pd.DataFrame, filePathFile = str) -> None:
    csvContent.to_csv(filePathFile)

def jsonNormalize(data, recordPath, meta, metaPrefix, recordPrefix, errors) -> pd.DataFrame:
    return pd.json_normalize(
        data=data,
        record_path=recordPath,
        meta=meta,
        meta_prefix=metaPrefix,
        record_prefix=recordPrefix,
        errors=errors
    )

def crossTab(index, columns)-> pd.DataFrame:
    return pd.crosstab(index,columns)


def fromDict(jsonContent, orient="index") -> pd.DataFrame:
    return  pd.DataFrame.from_dict(jsonContent, orient=orient)

def subset(data = pd.DataFrame, columns = [str]) -> pd.DataFrame:
    return data[columns]

def renameColumns(data = pd.DataFrame, renameColumns = {}, errors='ignore') -> pd.DataFrame:
    return data.rename(columns=renameColumns)

def saveParquetFile(data = pd.DataFrame, path = str) -> None:
    data.to_parquet(path, engine='fastparquet')

def addColumnIfNotExists(data = pd.DataFrame, column = str, default_value='') -> pd.DataFrame:
    if column not in data:
        data[column] = default_value

def to_datetime_key(data = pd.DataFrame, column = str):
    return data[column].astype(str).str.replace('-','')

def replace_null(data = pd.DataFrame, column = str, replace_value:any=None):
    data.loc[data[column].isnull(), column] = replace_value

def toDateTime(series = pd.Series) -> pd.Series:
    return pd.to_datetime(series)

def createDataFrame(data, columns) -> pd.DataFrame:
    return pd.DataFrame(
        data=data,
        columns=columns)
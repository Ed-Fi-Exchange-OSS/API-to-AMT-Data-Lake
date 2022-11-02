# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import os

import pandas as pd


def pdMerge(left=pd.DataFrame, right=pd.DataFrame, how=str, leftOn=[str], rigthOn=[str], suffixLeft='_x', suffixRight='_y') -> pd.DataFrame:
    return pd.merge(
        left,
        right,
        how=how,
        left_on=leftOn,
        right_on=rigthOn,
        suffixes=(suffixLeft, suffixRight)
    )


def pd_concat(objs=[pd.DataFrame], ignore_index=True):
    return pd.concat(objs, ignore_index=ignore_index)


# Use this method to review a dataframe content
def toCsv(csvContent=pd.DataFrame, path=str, file_name=str, school_year=str) -> None:
    school_year_path = f"{school_year}/" if school_year else ""
    destination_folder = os.path.join(path, school_year_path)
    destination_path = os.path.join(destination_folder, file_name)

    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder, exist_ok=True)

    csvContent.to_csv(destination_path)


def jsonNormalize(data, recordPath, meta, metaPrefix, recordPrefix, errors) -> pd.DataFrame:
    # Create an empty database with columns.
    empty_data_frame = create_empty_dataframe(meta)
    if not data:
        return empty_data_frame
    df_result = pd.json_normalize(
        data=data,
        record_path=recordPath,
        meta=meta,
        meta_prefix=metaPrefix,
        record_prefix=recordPrefix,
        errors=errors
    )
    return pd.concat([
        empty_data_frame,
        df_result
    ])


def create_empty_dataframe(columns=[]):
    dataframe_columns = []
    if columns:
        for column in columns:
            if not isinstance(column, list):
                dataframe_columns.append(
                    column
                )
            if isinstance(column, list) and len(column) == 2:
                dataframe_columns.append(
                    column[0]
                    + '.' + column[1]
                )
    return pd.DataFrame(columns=dataframe_columns)


def crossTab(index, columns) -> pd.DataFrame:
    return pd.crosstab(index, columns)


def fromDict(jsonContent, orient="index") -> pd.DataFrame:
    return pd.DataFrame.from_dict(jsonContent, orient=orient)


def subset(data=pd.DataFrame, columns=[str]) -> pd.DataFrame:
    return data[columns]


def renameColumns(data=pd.DataFrame, renameColumns={}, errors='ignore') -> pd.DataFrame:
    return data.rename(columns=renameColumns)


def saveParquetFile(data=pd.DataFrame, path=str, file_name=str, school_year=str) -> None:
    school_year_path = f"{school_year}/" if school_year else ""
    destination_folder = os.path.join(path, school_year_path)
    destination_path = os.path.join(destination_folder, file_name)

    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder, exist_ok=True)
    data.to_parquet(f"{destination_path}", engine='fastparquet')


def addColumnIfNotExists(data=pd.DataFrame, column=str, default_value='') -> pd.DataFrame:
    if column not in data:
        data[column] = default_value


def to_datetime_key(data=pd.DataFrame, column=str):
    return data[column].astype(str).str.replace('-', '')


def to_datetime(data=pd.DataFrame, column=str):
    return pd.to_datetime(data[column])


def replace_null(data=pd.DataFrame, column=str, replace_value: any = None):
    if not (column in data):
        data[column] = replace_value
    data.loc[data[column].isnull(), column] = replace_value


def toDateTime(series=pd.Series) -> pd.Series:
    return pd.to_datetime(series)


def createDataFrame(data, columns) -> pd.DataFrame:
    return pd.DataFrame(
        data=data,
        columns=columns)


def get_descriptor_code_value_from_uri(data=pd.DataFrame, column=str):
    if not (column in data):
        data[column] = ''
    if not data[column].empty:
        if len(data[column].str.split('#')) > 0:
            data[column] = data[column].str.split("#").str.get(-1)
    else:
        data[column] = ''


def get_reference_from_href(data=pd.DataFrame, column=str, destination_column=str):
    if column in data:
        if not data[column].empty:
            if len(data[column].str.split('/')) > 0:
                data[destination_column] = data[column].str.split('/').str.get(-1)
    # add the column if it was not added.
    if not (destination_column in data):
        data[destination_column] = ""

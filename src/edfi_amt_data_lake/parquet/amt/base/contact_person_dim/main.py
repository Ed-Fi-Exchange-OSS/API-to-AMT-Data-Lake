# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

import pandas as pd
from decouple import config
from edfi_amt_data_lake.parquet.Common.descriptor_mapping import get_descriptor_constant

from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    addColumnIfNotExists,
    get_descriptor_code_value_from_uri,
    get_reference_from_href,
    jsonNormalize,
    pdMerge,
    renameColumns,
    replace_null,
    saveParquetFile,
    subset,
    toCsv,
)

ENDPOINT_STUDENT_PARENT_ASSOCIATIONS = 'studentParentAssociations'
ENDPOINT_PARENTS = 'parents'


def contact_person_dim_dataframe(school_year) -> pd.DataFrame:
    student_parent_associations_content = getEndpointJson(ENDPOINT_STUDENT_PARENT_ASSOCIATIONS, config('SILVER_DATA_LOCATION'), school_year)
    parents_content = getEndpointJson(ENDPOINT_PARENTS, config('SILVER_DATA_LOCATION'), school_year)
    
    student_parent_associations_normalize = jsonNormalize(
        student_parent_associations_content,
        recordPath=None,
        meta=[
            'primaryContactStatus',
            'livesWith',
            'emergencyContactStatus',
            'contactPriority',
            'contactRestrictions',
            'relationDescriptor',
            ['parentReference', 'parentUniqueId'],
            ['studentReference', 'studentUniqueId']
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    toCsv(student_parent_associations_normalize, f"{config('PARQUET_FILES_LOCATION')}", "student_parent_associations_normalize.csv", school_year)

    parents_normalize = jsonNormalize(
        parents_content,
        recordPath=None,
        meta=[
            'id',
            'parentUniqueId',
            'firstName',
            'lastSurname'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    toCsv(parents_normalize, f"{config('PARQUET_FILES_LOCATION')}", "parents_normalize.csv", school_year)

    parents_address_normalize = jsonNormalize(
        parents_content,
        recordPath=[
            'addresses'
        ],
        meta=[
            'id'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    parents_address_normalize = get_descriptor_constant(parents_address_normalize, 'addressTypeDescriptor')

    parents_address_normalize = (
        parents_address_normalize[
            (parents_address_normalize['addressTypeDescriptor_constantName'].str.contains('Address.Home'))
            | (parents_address_normalize['addressTypeDescriptor_constantName'].str.contains('Address.Physical'))
            | (parents_address_normalize['addressTypeDescriptor_constantName'].str.contains('Address.Mailing'))
            | (parents_address_normalize['addressTypeDescriptor_constantName'].str.contains('Address.Work'))
            | (parents_address_normalize['addressTypeDescriptor_constantName'].str.contains('Address.Temporary'))
        ]
    )

    addColumnIfNotExists(parents_address_normalize, 'apartmentRoomSuiteNumber')

    get_descriptor_code_value_from_uri(parents_address_normalize, 'stateAbbreviationDescriptor')

    parents_address_normalize['Address'] = (
        parents_address_normalize['streetNumberName']
        + ', '
        + parents_address_normalize['apartmentRoomSuiteNumber']
        + ', '
        + parents_address_normalize['city']
        + ' '
        + parents_address_normalize['stateAbbreviationDescriptor']
        + ' '
        + parents_address_normalize['postalCode']
    )

    toCsv(parents_address_normalize, f"{config('PARQUET_FILES_LOCATION')}", "parents_address_normalize.csv", school_year)

    parents_telephones_normalize = jsonNormalize(
        parents_content,
        recordPath=[
            'telephones'
        ],
        meta=[
            'id'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    parents_telephones_normalize = get_descriptor_constant(parents_telephones_normalize, 'telephoneNumberTypeDescriptor')

    parents_telephones_normalize = (
        parents_telephones_normalize[
            (parents_telephones_normalize['telephoneNumberTypeDescriptor_constantName'].str.contains('Telephone.Home'))
            | (parents_telephones_normalize['telephoneNumberTypeDescriptor_constantName'].str.contains('Telephone.Mobile'))
            | (parents_telephones_normalize['telephoneNumberTypeDescriptor_constantName'].str.contains('Telephone.Work'))
        ]
    )

    toCsv(parents_telephones_normalize, f"{config('PARQUET_FILES_LOCATION')}", "parents_telephones_normalize.csv", school_year)

    parents_electronicMails_normalize = jsonNormalize(
        parents_content,
        recordPath=[
            'electronicMails'
        ],
        meta=[
            'id'
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    parents_electronicMails_normalize = get_descriptor_constant(parents_electronicMails_normalize, 'electronicMailTypeDescriptor')

    parents_electronicMails_normalize = (
        parents_electronicMails_normalize[
            (parents_electronicMails_normalize['electronicMailTypeDescriptor_constantName'].str.contains('Email.Personal'))
            | (parents_electronicMails_normalize['electronicMailTypeDescriptor_constantName'].str.contains('Email.Work'))
        ]
    )

    toCsv(parents_electronicMails_normalize, f"{config('PARQUET_FILES_LOCATION')}", "parents_electronicMails_normalize.csv", school_year)

    student_parent_associations_normalize['_parents'] = '|'

    result_data_frame = pdMerge(
        left=student_parent_associations_normalize,
        right=parents_normalize,
        how='inner',
        leftOn=['parentReference.parentUniqueId'],
        rigthOn=['parentUniqueId'],
        suffixLeft=None,
        suffixRight='_parents'
    )

    result_data_frame['_parents_address'] = '|'

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=parents_address_normalize,
        how='left',
        leftOn=['id_parents'],
        rigthOn=['id'],
        suffixLeft=None,
        suffixRight='_parents_address'
    )

    result_data_frame['_parents_phones'] = '|'

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=parents_telephones_normalize,
        how='left',
        leftOn=['id_parents'],
        rigthOn=['id'],
        suffixLeft=None,
        suffixRight='_parents_phones'
    )

    result_data_frame['_parents_mails'] = '|'

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=parents_electronicMails_normalize,
        how='left',
        leftOn=['id_parents'],
        rigthOn=['id'],
        suffixLeft=None,
        suffixRight='_parents_mails'
    )

    return result_data_frame


def contact_person_dim(school_year) -> None:
    result_data_frame = contact_person_dim_dataframe(school_year)
    # saveParquetFile(result_data_frame, f"{config('PARQUET_FILES_LOCATION')}", "contactPersonDim.parquet", school_year)
    toCsv(result_data_frame, f"{config('PARQUET_FILES_LOCATION')}", "contactPersonDim.csv", school_year)

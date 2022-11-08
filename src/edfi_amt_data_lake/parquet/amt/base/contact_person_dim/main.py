# SPDX-License-Identifier: Apache-2.0
# Licensed to the Ed-Fi Alliance under one or more agreements.
# The Ed-Fi Alliance licenses this file to you under the Apache License, Version 2.0.
# See the LICENSE and NOTICES files in the project root for more information.

from datetime import date

import pandas as pd
from decouple import config

from edfi_amt_data_lake.parquet.Common.descriptor_mapping import get_descriptor_constant
from edfi_amt_data_lake.parquet.Common.functions import getEndpointJson
from edfi_amt_data_lake.parquet.Common.pandasWrapper import (
    addColumnIfNotExists,
    get_descriptor_code_value_from_uri,
    jsonNormalize,
    pdMerge,
    renameColumns,
    saveParquetFile,
    subset,
    to_datetime_key,
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

    addColumnIfNotExists(student_parent_associations_normalize, 'primaryContactStatus', False)
    addColumnIfNotExists(student_parent_associations_normalize, 'livesWith', False)
    addColumnIfNotExists(student_parent_associations_normalize, 'emergencyContactStatus', False)
    addColumnIfNotExists(student_parent_associations_normalize, 'contactPriority', 0)
    addColumnIfNotExists(student_parent_associations_normalize, 'contactRestrictions', '')
    addColumnIfNotExists(student_parent_associations_normalize, 'relationDescriptor', '')
    get_descriptor_code_value_from_uri(student_parent_associations_normalize, 'relationDescriptor')

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

    parents_address_normalize = parents_address_normalize.fillna('')

    get_descriptor_code_value_from_uri(parents_address_normalize, 'stateAbbreviationDescriptor')

    # Address periods
    parents_address_periods_normalize = jsonNormalize(
        parents_content,
        recordPath=[
            'addresses', 'periods'
        ],
        meta=[
            'id',
            ['addresses', 'addressTypeDescriptor']
        ],
        metaPrefix=None,
        recordPrefix=None,
        errors='ignore'
    )

    parents_address_normalize['_parents_address_periods'] = '|'

    parents_address_normalize = pdMerge(
        left=parents_address_normalize,
        right=parents_address_periods_normalize,
        how='left',
        leftOn=['id', 'addressTypeDescriptor'],
        rigthOn=['id', 'addresses.addressTypeDescriptor'],
        suffixLeft=None,
        suffixRight='_parents_address_periods'
    )

    if 'endDate' in parents_address_normalize:
        parents_address_normalize['endDate'] = to_datetime_key(parents_address_normalize, 'endDate')
        parents_address_normalize['date_now'] = date.today()
        parents_address_normalize['date_now'] = to_datetime_key(parents_address_normalize, 'date_now')
        parents_address_normalize = parents_address_normalize[parents_address_normalize['endDate'] >= parents_address_normalize['date_now']]

    addColumnIfNotExists(parents_address_normalize, 'apartmentRoomSuiteNumber', '')

    parents_address_normalize['Address'] = (
        parents_address_normalize['streetNumberName']
    )

    parents_address_normalize["Address"] = parents_address_normalize.apply(
        lambda r: (r["Address"] + ', ' + r["apartmentRoomSuiteNumber"]) if r["apartmentRoomSuiteNumber"] != '' else r["Address"], axis=1
    )

    parents_address_normalize["Address"] = (
        parents_address_normalize["Address"]
        + ', '
        + parents_address_normalize['city']
        + ' '
        + parents_address_normalize['stateAbbreviationDescriptor']
        + ' '
        + parents_address_normalize['postalCode']
    )

    # Parent Address - Home
    parents_address_normalize_home = (
        parents_address_normalize[
            (parents_address_normalize['addressTypeDescriptor_constantName'].str.contains('Address.Home'))
        ]
    )

    # Parent Address - Physical
    parents_address_normalize_physical = (
        parents_address_normalize[
            (parents_address_normalize['addressTypeDescriptor_constantName'].str.contains('Address.Physical'))
        ]
    )

    # Parent Address - Mailing
    parents_address_normalize_mailing = (
        parents_address_normalize[
            (parents_address_normalize['addressTypeDescriptor_constantName'].str.contains('Address.Mailing'))
        ]
    )

    # Parent Address - Work
    parents_address_normalize_work = (
        parents_address_normalize[
            (parents_address_normalize['addressTypeDescriptor_constantName'].str.contains('Address.Work'))
        ]
    )

    # Parent Address - Temporary
    parents_address_normalize_temporary = (
        parents_address_normalize[
            (parents_address_normalize['addressTypeDescriptor_constantName'].str.contains('Address.Temporary'))
        ]
    )

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

    result_data_frame['address_home'] = '|'

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=parents_address_normalize_home,
        how='left',
        leftOn=['id_parents'],
        rigthOn=['id'],
        suffixLeft=None,
        suffixRight='_address_home'
    )

    result_data_frame['address_physical'] = '|'

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=parents_address_normalize_physical,
        how='left',
        leftOn=['id_parents'],
        rigthOn=['id'],
        suffixLeft=None,
        suffixRight='_address_physical'
    )

    result_data_frame['address_mailing'] = '|'

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=parents_address_normalize_mailing,
        how='left',
        leftOn=['id_parents'],
        rigthOn=['id'],
        suffixLeft=None,
        suffixRight='_address_mailing'
    )

    result_data_frame['address_work'] = '|'

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=parents_address_normalize_work,
        how='left',
        leftOn=['id_parents'],
        rigthOn=['id'],
        suffixLeft=None,
        suffixRight='_address_work'
    )

    result_data_frame['address_temporary'] = '|'

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=parents_address_normalize_temporary,
        how='left',
        leftOn=['id_parents'],
        rigthOn=['id'],
        suffixLeft=None,
        suffixRight='_address_temporary'
    )

    result_data_frame = subset(result_data_frame, [
        'id',
        'emergencyContactStatus',
        'livesWith',
        'primaryContactStatus',
        'relationDescriptor',
        'studentReference.studentUniqueId',
        'contactPriority',
        'contactRestrictions',
        'id_parents',
        'parentUniqueId',
        'firstName',
        'lastSurname',
        'Address',
        'postalCode',
        'Address_address_physical',
        'Address_address_mailing',
        'Address_address_work',
        'Address_address_temporary',
    ])

    # Parent Telephones
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

    parents_telephones_normalize = parents_telephones_normalize.fillna('')

    parents_telephones_normalize_home = (
        parents_telephones_normalize[
            (parents_telephones_normalize['telephoneNumberTypeDescriptor_constantName'].str.contains('Telephone.Home'))
        ]
    )

    parents_telephones_normalize_mobile = (
        parents_telephones_normalize[
            (parents_telephones_normalize['telephoneNumberTypeDescriptor_constantName'].str.contains('Telephone.Mobile'))
        ]
    )

    parents_telephones_normalize_work = (
        parents_telephones_normalize[
            (parents_telephones_normalize['telephoneNumberTypeDescriptor_constantName'].str.contains('Telephone.Work'))
        ]
    )

    result_data_frame['phones_home'] = '|'

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=parents_telephones_normalize_home,
        how='left',
        leftOn=['id_parents'],
        rigthOn=['id'],
        suffixLeft=None,
        suffixRight='_phones_home'
    )

    result_data_frame['phones_mobile'] = '|'

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=parents_telephones_normalize_mobile,
        how='left',
        leftOn=['id_parents'],
        rigthOn=['id'],
        suffixLeft=None,
        suffixRight='_phones_mobile'
    )

    result_data_frame['phones_work'] = '|'

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=parents_telephones_normalize_work,
        how='left',
        leftOn=['id_parents'],
        rigthOn=['id'],
        suffixLeft=None,
        suffixRight='_phones_work'
    )

    result_data_frame = subset(result_data_frame, [
        'id',
        'emergencyContactStatus',
        'livesWith',
        'primaryContactStatus',
        'relationDescriptor',
        'studentReference.studentUniqueId',
        'contactPriority',
        'contactRestrictions',
        'id_parents',
        'parentUniqueId',
        'firstName',
        'lastSurname',
        'Address',
        'postalCode',
        'Address_address_physical',
        'Address_address_mailing',
        'Address_address_work',
        'Address_address_temporary',
        'telephoneNumber',
        'telephoneNumber_phones_mobile',
        'telephoneNumber_phones_work'
    ])

    # Parent Emails
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

    addColumnIfNotExists(parents_electronicMails_normalize, 'primaryEmailAddressIndicator', False)

    parents_electronicMails_normalize = get_descriptor_constant(parents_electronicMails_normalize, 'electronicMailTypeDescriptor')

    parents_electronicMails_normalize = parents_electronicMails_normalize.fillna('')

    parents_electronicMails_normalize_personal = (
        parents_electronicMails_normalize[
            (parents_electronicMails_normalize['electronicMailTypeDescriptor_constantName'].str.contains('Email.Personal'))
        ]
    )

    parents_electronicMails_normalize_work = (
        parents_electronicMails_normalize[
            (parents_electronicMails_normalize['electronicMailTypeDescriptor_constantName'].str.contains('Email.Work'))
        ]
    )

    result_data_frame['_parents_mails_work'] = '|'

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=parents_electronicMails_normalize_work,
        how='left',
        leftOn=['id_parents'],
        rigthOn=['id'],
        suffixLeft=None,
        suffixRight='_parents_mails_work'
    )

    result_data_frame['_parents_mails_personal'] = '|'

    result_data_frame = pdMerge(
        left=result_data_frame,
        right=parents_electronicMails_normalize_personal,
        how='left',
        leftOn=['id_parents'],
        rigthOn=['id'],
        suffixLeft=None,
        suffixRight='_parents_mails_personal'
    )

    # Primary Email Address
    result_data_frame['PrimaryEmailAddress'] = 'Not specified'

    # Parent Email - Work Email - Primary Email Address - Work
    result_data_frame["PrimaryEmailAddress"] = result_data_frame.apply(
        lambda r: ('Work') if r["primaryEmailAddressIndicator"] else 'Not specified', axis=1
    )

    # Parent Email - Work Email - Primary Email Address - Personal
    result_data_frame["PrimaryEmailAddress"] = result_data_frame.apply(
        lambda r: ('Personal') if r["primaryEmailAddressIndicator_parents_mails_personal"] else 'Not specified', axis=1
    )

    result_data_frame = subset(result_data_frame, [
        'id',
        'emergencyContactStatus',
        'livesWith',
        'primaryContactStatus',
        'relationDescriptor',
        'studentReference.studentUniqueId',
        'contactPriority',
        'contactRestrictions',
        'id_parents',
        'parentUniqueId',
        'firstName',
        'lastSurname',
        'Address',
        'postalCode',
        'Address_address_physical',
        'Address_address_mailing',
        'Address_address_work',
        'Address_address_temporary',
        'telephoneNumber',
        'telephoneNumber_phones_mobile',
        'telephoneNumber_phones_work',
        'electronicMailAddress',
        'electronicMailAddress_parents_mails_personal',
        'PrimaryEmailAddress'
    ])

    result_data_frame['livesWith'] = result_data_frame['livesWith'].fillna(False)

    result_data_frame["primaryContactStatus"] = result_data_frame["primaryContactStatus"].astype(int)
    result_data_frame["livesWith"] = result_data_frame["livesWith"].astype(int)
    result_data_frame["emergencyContactStatus"] = result_data_frame["emergencyContactStatus"].astype(int)

    result_data_frame['UniqueKey'] = result_data_frame['parentUniqueId'] + '-' + result_data_frame['studentReference.studentUniqueId']

    result_data_frame = renameColumns(result_data_frame, {
        'parentUniqueId': 'ContactPersonKey',
        'studentReference.studentUniqueId': 'StudentKey',
        'firstName': 'ContactFirstName',
        'lastSurname': 'ContactLastName',
        'relationDescriptor': 'RelationshipToStudent',
        'Address': 'ContactHomeAddress',
        'Address_address_physical': 'ContactPhysicalAddress',
        'Address_address_mailing': 'ContactMailingAddress',
        'Address_address_work': 'ContactWorkAddress',
        'Address_address_temporary': 'ContactTemporaryAddress',
        'telephoneNumber': 'HomePhoneNumber',
        'telephoneNumber_phones_mobile': 'MobilePhoneNumber',
        'telephoneNumber_phones_work': 'WorkPhoneNumber',
        'electronicMailAddress': 'WorkEmailAddress',
        'electronicMailAddress_parents_mails_personal': 'PersonalEmailAddress',
        'primaryContactStatus': 'IsPrimaryContact',
        'livesWith': 'StudentLivesWith',
        'emergencyContactStatus': 'IsEmergencyContact',
        'contactPriority': 'ContactPriority',
        'contactRestrictions': 'ContactRestrictions',
        'postalCode': 'PostalCode'
    })

    result_data_frame = result_data_frame[[
        'UniqueKey',
        'ContactPersonKey',
        'StudentKey',
        'ContactFirstName',
        'ContactLastName',
        'RelationshipToStudent',
        'ContactHomeAddress',
        'ContactPhysicalAddress',
        'ContactMailingAddress',
        'ContactWorkAddress',
        'ContactTemporaryAddress',
        'HomePhoneNumber',
        'MobilePhoneNumber',
        'WorkPhoneNumber',
        'PrimaryEmailAddress',
        'PersonalEmailAddress',
        'WorkEmailAddress',
        'IsPrimaryContact',
        'StudentLivesWith',
        'IsEmergencyContact',
        'ContactPriority',
        'ContactRestrictions',
        'PostalCode'
    ]]

    return result_data_frame


def contact_person_dim(school_year) -> None:
    result_data_frame = contact_person_dim_dataframe(school_year)
    saveParquetFile(result_data_frame, f"{config('PARQUET_FILES_LOCATION')}", "contactPersonDim.parquet", school_year)

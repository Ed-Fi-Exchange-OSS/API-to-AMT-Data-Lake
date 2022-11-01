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
    toDateTime,
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

    # Parent Address

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

    # Parent Address - Home Address
    parents_address_normalize['Address.Home_Address'] = ''
    mask = parents_address_normalize["addressTypeDescriptor_constantName"] == 'Address.Home'
    parents_address_normalize.loc[mask, "Address.Home_Address"] = parents_address_normalize.loc[mask, "Address"]

    # Parent Address - Home Address - Posttal Code
    parents_address_normalize['Address.Home_Address_Postalode'] = ''
    mask = parents_address_normalize["addressTypeDescriptor_constantName"] == 'Address.Home'
    parents_address_normalize.loc[mask, "Address.Home_Address_PostalCode"] = parents_address_normalize.loc[mask, "postalCode"]

    # Parent Address - Physical Address
    parents_address_normalize['Address.Physical_Address'] = ''
    mask = parents_address_normalize["addressTypeDescriptor_constantName"] == 'Address.Physical'
    parents_address_normalize.loc[mask, "Address.Physical_Address"] = parents_address_normalize.loc[mask, "Address"]

    # Parent Address - Mailing Address
    parents_address_normalize['Address.Mailing_Address'] = ''
    mask = parents_address_normalize["addressTypeDescriptor_constantName"] == 'Address.Mailing'
    parents_address_normalize.loc[mask, "Address.Mailing_Address"] = parents_address_normalize.loc[mask, "Address"]

    # Parent Address - Temporary Address
    parents_address_normalize['Address.Temporary_Address'] = ''
    mask = parents_address_normalize["addressTypeDescriptor_constantName"] == 'Address.Temporary'
    parents_address_normalize.loc[mask, "Address.Temporary_Address"] = parents_address_normalize.loc[mask, "Address"]

    # Parent Address - Work Address
    parents_address_normalize['Address.Work_Address'] = ''
    mask = parents_address_normalize["addressTypeDescriptor_constantName"] == 'Address.Work'
    parents_address_normalize.loc[mask, "Address.Work_Address"] = parents_address_normalize.loc[mask, "Address"]

    ## Address periods
    
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

    addColumnIfNotExists(parents_address_periods_normalize, 'endDate')
    # parents_address_periods_normalize['endDate.datetime'] = toDateTime(parents_address_periods_normalize['endDate'])

    parents_address_normalize = pdMerge(
        left=parents_address_normalize,
        right=parents_address_periods_normalize,
        how='left',
        leftOn=['id', 'addressTypeDescriptor'],
        rigthOn=['id', 'addresses.addressTypeDescriptor'],
        suffixLeft=None,
        suffixRight='_parents_address_periods'
    )

    # ToDo: Apply filter
        # WHERE
        #     ParentAddressPeriod.EndDate IS NULL
        # OR
        #     ParentAddressPeriod.EndDate > GETDATE()

    toCsv(parents_address_periods_normalize, f"{config('PARQUET_FILES_LOCATION')}", "parents_address_periods_normalize.csv", school_year)

    toCsv(parents_address_normalize, f"{config('PARQUET_FILES_LOCATION')}", "parents_address_normalize.csv", school_year)

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

    parents_telephones_normalize = (
        parents_telephones_normalize[
            (parents_telephones_normalize['telephoneNumberTypeDescriptor_constantName'].str.contains('Telephone.Home'))
            | (parents_telephones_normalize['telephoneNumberTypeDescriptor_constantName'].str.contains('Telephone.Mobile'))
            | (parents_telephones_normalize['telephoneNumberTypeDescriptor_constantName'].str.contains('Telephone.Work'))
        ]
    )

    # Parent Telephone - Home
    parents_telephones_normalize['Telephone.Home_Telephone'] = ''
    mask = parents_telephones_normalize["telephoneNumberTypeDescriptor_constantName"] == 'Telephone.Home'
    parents_telephones_normalize.loc[mask, "Telephone.Home_Telephone"] = parents_telephones_normalize.loc[mask, "telephoneNumber"]

    # Parent Telephone - Mobile
    parents_telephones_normalize['Telephone.Mobile_Telephone'] = ''
    mask = parents_telephones_normalize["telephoneNumberTypeDescriptor_constantName"] == 'Telephone.Mobile'
    parents_telephones_normalize.loc[mask, "Telephone.Mobile_Telephone"] = parents_telephones_normalize.loc[mask, "telephoneNumber"]

    # Parent Telephone - Work
    parents_telephones_normalize['Telephone.Work_Telephone'] = ''
    mask = parents_telephones_normalize["telephoneNumberTypeDescriptor_constantName"] == 'Telephone.Work'
    parents_telephones_normalize.loc[mask, "Telephone.Work_Telephone"] = parents_telephones_normalize.loc[mask, "telephoneNumber"]

    toCsv(parents_telephones_normalize, f"{config('PARQUET_FILES_LOCATION')}", "parents_telephones_normalize.csv", school_year)


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

    parents_electronicMails_normalize = (
        parents_electronicMails_normalize[
            (parents_electronicMails_normalize['electronicMailTypeDescriptor_constantName'].str.contains('Email.Personal'))
            | (parents_electronicMails_normalize['electronicMailTypeDescriptor_constantName'].str.contains('Email.Work'))
        ]
    )

    # Parent Email - Work Email
    parents_electronicMails_normalize['Email.Work_Email'] = ''
    mask = parents_electronicMails_normalize["electronicMailTypeDescriptor_constantName"] == 'Email.Work'
    parents_electronicMails_normalize.loc[mask, "Email.Work_Email"] = parents_electronicMails_normalize.loc[mask, "electronicMailAddress"]

    # Parent Email - Personal Email
    parents_electronicMails_normalize['Email.Personal_Email'] = ''
    mask = parents_electronicMails_normalize["electronicMailTypeDescriptor_constantName"] == 'Email.Personal'
    parents_electronicMails_normalize.loc[mask, "Email.Personal_Email"] = parents_electronicMails_normalize.loc[mask, "electronicMailAddress"]

    # Primary Email Address
    parents_electronicMails_normalize['PrimaryEmailAddress_email'] = 'Not specified'
    parents_electronicMails_normalize['primaryEmailAddressIndicator'] = parents_electronicMails_normalize['primaryEmailAddressIndicator'].astype(str)
    # Parent Email - Work Email - Primary Email Address - Work
    # parents_electronicMails_normalize.loc[
    #     parents_electronicMails_normalize['primaryEmailAddressIndicator'] == 'true'
    #     & parents_electronicMails_normalize['electronicMailTypeDescriptor_constantName'] == 'Email.Work', 'PrimaryEmailAddress_email'
    # ] = 'Work'

    # Parent Email - Work Email - Primary Email Address - Personal
    # parents_electronicMails_normalize.loc[
    #     parents_electronicMails_normalize['primaryEmailAddressIndicator'].str.lower() == 'true' 
    #     & parents_electronicMails_normalize['electronicMailTypeDescriptor_constantName'] == 'Email.Personal', 'PrimaryEmailAddress_email'
    # ] = 'Personal'

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

    result_data_frame = subset(result_data_frame, [
        'parentUniqueId',
        'studentReference.studentUniqueId',
        'firstName',
        'lastSurname',
        'relationDescriptor',
        'Address.Home_Address',
        'Address.Physical_Address',
        'Address.Mailing_Address',
        'Address.Work_Address',
        'Address.Temporary_Address',
        'Telephone.Home_Telephone',
        'Telephone.Mobile_Telephone',
        'Telephone.Work_Telephone',
        'PrimaryEmailAddress_email',
        'Email.Work_Email',
        'Email.Personal_Email',
        'primaryContactStatus',
        'livesWith',
        'emergencyContactStatus',
        'contactPriority',
        'contactRestrictions',
        'Address.Home_Address_PostalCode'
    ])

    result_data_frame['UniqueKey'] = result_data_frame['parentUniqueId'] + result_data_frame['studentReference.studentUniqueId']
    
    result_data_frame = renameColumns(result_data_frame, {
        'parentUniqueId': 'ContactPersonKey',
        'studentReference.studentUniqueId': 'StudentKey',
        'firstName': 'ContactFirstName',
        'lastSurname': 'ContactLastName',
        'relationDescriptor': 'RelationshipToStudent',
        'Address.Home_Address': 'ContactHomeAddress',
        'Address.Physical_Address': 'ContactPhysicalAddress',
        'Address.Mailing_Address': 'ContactMailingAddress',
        'Address.Work_Address': 'ContactWorkAddress',
        'Address.Temporary_Address': 'ContactTemporaryAddress',
        'Telephone.Home_Telephone': 'HomePhoneNumber',
        'Telephone.Mobile_Telephone': 'MobilePhoneNumber',
        'Telephone.Work_Telephone': 'WorkPhoneNumber',
        'Email.Work_Email': 'WorkEmailAddress',
        'Email.Personal_Email': 'PersonalEmailAddress',
        'primaryContactStatus': 'IsPrimaryContact',
        'livesWith': 'StudentLivesWith',
        'emergencyContactStatus': 'IsEmergencyContact',
        'contactPriority': 'ContactPriority',
        'contactRestrictions': 'ContactRestrictions',
        'Address.Home_Address_PostalCode': 'PostalCode'
    })

    return result_data_frame


def contact_person_dim(school_year) -> None:
    result_data_frame = contact_person_dim_dataframe(school_year)
    # saveParquetFile(result_data_frame, f"{config('PARQUET_FILES_LOCATION')}", "contactPersonDim.parquet", school_year)
    toCsv(result_data_frame, f"{config('PARQUET_FILES_LOCATION')}", "contactPersonDim.csv", school_year)

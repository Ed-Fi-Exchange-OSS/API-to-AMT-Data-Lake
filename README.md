# Analytics-Middle-Tier-Data-Lake

## API to AMT Data Lake
Extracts all data exposed by the Ed-Fi API and saves it into JSON files in order to be consumed and transformed (on a second phase) in another project.

## Getting Started

1. Requires Python 3.9+ and Poetry.
1. Install required Python packages.

## Local environment
Authentication is necessary to be able to download the different collections.


#### Environment variable configuration:

1. Rename the .env.example file to .env, or generate an .env file using .env.example as a reference.
2. Modify the .env file to include your information.


```shi
---------------------------------------------------
OS_CPU=4
---------------------------------------------------
PREX_DATA_V=data/v3
PREX_TOKEN=oauth/token
---------------------------------------------------
API_KEY=<your_secret_key>
API_SECRET=<your_secret_password>
API_URL=https://example.com/v5.3/api/
---------------------------------------------------
API_LIMIT=500
LOG_LEVEL=info
---------------------------------------------------
API_MODE=SharedInstance
SCHOOL_YEAR=2021,2022
---------------------------------------------------
CHANGE_VERSION_FILEPATH=<path_to_file>
CHANGE_VERSION_FILENAME=<name_of_version_file>
---------------------------------------------------
SILVER_DATA_LOCATION=<path_where_json_files_will_be_saved>
PARQUET_FILES_LOCATION=C:\\temp\\edfi\\parquet\\
---------------------------------------------------
```
**API_MODE:** SharedInstance or YearSpecific mode.

**SCHOOL_YEAR:** If API_MODE is YearSpecific, you must set a list of school years that you want to load, separated by commas..

**CHANGE_VERSION_FILENAME:** Version change file name.

**CHANGE_VERSION_FILEPATH:** The location where the changeVersion.txt file will be saved.

**OS_CPU:** Defined as the number of CPUs to be used for parallel calls, this value must be less than the number of CPUs of the machine for proper performance, **by default is 4.**

**SILVER_DATA_LOCATION:** The location where the raw data will be saved., The raw data is a collection of json files in an staging phase.

**PARQUET_FILES_LOCATION:** The location where the data in its final structure will be stored.

**SILVER_DATA_LOCATION:** The location where the raw data will be saved., The raw data is a collection of json files in an staging phase.

**PARQUET_FILES_LOCATION:** The location where the data in its final structure will be stored.

####  Continue with the next steps:

```sh
cd API-to-AMT-Data-Lake/src

poetry install

source $(poetry env info --path)/bin/activate

cd ../src/dagster_data_lake

dagit -w dagster/workspace.yaml
```

## Extra Steps

After you have finished the above steps, you can open your localhost or http://127.0.0.1:3000 and you will be able to see in the launcher the list of tests or collections available.

## Descriptor Mapping

  *"Some views in the Analytics Middle Tier need to filter data by a string value. For example, a query on the attendance events table looking only for the excused absences uses a string value to represent the concept of excused absences. This is because the Ed-Fi ODS database provides enumerations such as "Excused Absence" and "Unexcused Absence" via Ed-Fi Descriptors. Descriptors are customizable, and so each installation of the Ed-Fi ODS may have different descriptor values for the same "universal" concept. Therefore, the views cannot simply include a preset, static filter for the concepts expressed by a Descriptor value.*

  *To get around this variability, the Analytics Middle Tier introduces a DescriptorConstant table to represent those universal values. Each installation of the Analytics Middle Tier must then map its Descriptors to the DescriptorConstants as appropriate for the situation, via the DescriptorMap table."* [Descriptor Mapping](https://techdocs.ed-fi.org/display/EDFITOOLS/Descriptor+Mapping)

For the generation of the parquet files equivalent to the AMT views, the mapping of the descriptors with the constants is done through a json file. In this file, the relationships between the descriptors and their constants can be updated for the filters to work.

On this page you can find more context about the descriptors: [Descriptor Mapping](https://techdocs.ed-fi.org/display/EDFITOOLS/Descriptor+Mapping)


To update the list of constants, you can edit the [descriptor_map.json](./src/edfi_amt_data_lake/helper/descriptor_map/descriptor_map.json) file located at:

`.\src\edfi_amt_data_lake\helper\descriptor_map\descriptor_map.json`

The following properties must be included in the file for each row:
* **constantName:** The name of the constant according to the Descriptor Mapping documentation. **It is case sensitive.**
* **descriptor:** The name of the descriptor type to which the constant is associated. It is not case sensitive.
* **codeValue:** The specific value for the assigned descriptor. It is not case sensitive.

### Get descriptor mapping values from database
If the database to which the API connects has AMT installed and the descriptor tables updated, you can generate the json to copy it to the project file, with the following query

**MSSQL**
```SQL
SELECT constantName
	,descriptor
	,codeValue
FROM(
    SELECT DescriptorConstant.ConstantName as constantName
        ,REVERSE(SUBSTRING(REVERSE([Namespace]), 1, CHARINDEX('/', REVERSE([Namespace])) - 1)) AS descriptor
        ,Descriptor.CodeValue as codeValue
    FROM analytics_config.DescriptorMap
    INNER JOIN analytics_config.DescriptorConstant ON
        analytics_config.DescriptorMap.DescriptorConstantId = analytics_config.DescriptorConstant.DescriptorConstantId
    INNER JOIN edfi.Descriptor ON
        analytics_config.DescriptorMap.DescriptorId = edfi.Descriptor.DescriptorId
) AS descriptorMapping
FOR JSON AUTO
```
**PostgreSQL**
```SQL
SELECT json_agg(descriptorMapping)
FROM(
    SELECT DescriptorConstant.ConstantName as constantName
    ,REVERSE(SPLIT_PART(REVERSE(Namespace), '/', 1)) AS descriptor
        ,Descriptor.CodeValue as codeValue
    FROM analytics_config.DescriptorMap
    INNER JOIN analytics_config.DescriptorConstant ON
        analytics_config.DescriptorMap.DescriptorConstantId = analytics_config.DescriptorConstant.DescriptorConstantId
    INNER JOIN edfi.Descriptor ON
        analytics_config.DescriptorMap.DescriptorId = edfi.Descriptor.DescriptorId
) AS descriptorMapping
```

## Dev Operations

To validate the files you can execute :

- `pre-commit run --all-files`

It serves to make a previous validation of all the files, in addition to validate the files before committing them.

![Getting Started](https://raw.githubusercontent.com/Ed-Fi-Exchange-OSS/API-to-AMT-Data-Lake/BIA-1218/docs/images/04bee0340fb6fbee51a0ff8e017cb1c9696e09587a11a0c7e0a3e5d71ac03778.png)


1. Style check: `poetry run flake8`
2. Static typing check: `poetry run mypy .`
3. Run unit tests: `poetry run pytest`

## Legal Information

Copyright (c) 2021 Ed-Fi Alliance, LLC and contributors.

Licensed under the [Apache License, Version 2.0](LICENSE) (the "License").

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

See [NOTICES](NOTICES.md) for additional copyright and license notifications.

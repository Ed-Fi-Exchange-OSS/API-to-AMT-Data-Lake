# Analytics-Middle-Tier-Data-Lake

## API to AMT Data Lake
Project oriented to obtain the different collections from the AMT API in order to convert them into parquet files to be used with Dagster.

## Local environment
Authentication is necessary to be able to download the different collections.

Complete your .env file.
```sh
API_USER=<api_user>
API_PASSWORD=<api_password>
API_URL=<api_url>
```

Continue with the next steps:

```sh
cd API-to-AMT-Data-Lake/src
poetry install;
source $(poetry env info --path)/bin/activate

cd ../src/dagster_data_lake
dagit -w dagster/workspace.yaml;
```


## Legal Information

Copyright (c) 2021 Ed-Fi Alliance, LLC and contributors.

Licensed under the [Apache License, Version 2.0](LICENSE) (the "License").

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

See [NOTICES](NOTICES.md) for additional copyright and license notifications.

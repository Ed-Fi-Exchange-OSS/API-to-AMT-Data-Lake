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
CHANGE_VERSION_FILEPATH=<path_to_file>
```

CHANGE_VERSION_FILEPATH is the location where the changeVersion.txt file will be saved.

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

### Dev Operations

1. Style check: `poetry run flake8`
2. Static typing check: `poetry run mypy`
3. Run unit tests: `poetry run pytest`


## Legal Information

Copyright (c) 2021 Ed-Fi Alliance, LLC and contributors.

Licensed under the [Apache License, Version 2.0](LICENSE) (the "License").

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

See [NOTICES](NOTICES.md) for additional copyright and license notifications.

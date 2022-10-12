from dagster_data_lake.ops.api import api


def test_api() -> bool:
    assert api() == list()

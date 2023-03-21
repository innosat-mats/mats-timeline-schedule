import os
from tempfile import TemporaryDirectory
from unittest.mock import ANY, Mock, patch

import pytest  # type: ignore
from mats_schedule.handlers.mats_schedule import (
    download_file,
    get_filename,
    get_or_raise,
    parse_event_message,
)


@patch.dict(os.environ, {"DEFINITELY": "set"})
def test_get_or_raise():
    assert get_or_raise("DEFINITELY") == "set"


def test_get_or_raise_raises():
    with pytest.raises(
        EnvironmentError,
        match="DEFINITELYNOT is a required environment variable"
    ):
        get_or_raise("DEFINITELYNOT")


def test_parse_event_message():
    event = {
        "Records": [
            {
                "body": '{"bucket": "some-bucket", "object": "path/to/file.csv"}'  # noqa: E501
            }
        ]
    }
    assert parse_event_message(event) == ("path/to/file.csv", "some-bucket")


def test_download_file():
    mocked_client = Mock()
    bucket_name = "bucket"
    file_name = "file1.csv"
    output_dir = TemporaryDirectory()
    download_file(mocked_client, bucket_name, file_name, output_dir)
    mocked_client.download_file.assert_called_once_with(
        'bucket',
        'file1.csv',
        ANY,
    )


def test_get_filename():
    csv_filename = "20230309_timeline_schedule.csv"
    parquet_filename = "20230309_timeline_schedule_{i}.parquet"
    assert get_filename(csv_filename) == parquet_filename

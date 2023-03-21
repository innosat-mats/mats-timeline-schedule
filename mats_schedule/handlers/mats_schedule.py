
import json
import logging
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Callable, Dict, Optional, Tuple

import boto3
import h5py  # type: ignore
import numpy as np
import pyarrow as pa  # type: ignore
import pyarrow.parquet as pq  # type: ignore
import pyarrow.csv as csv  # type: ignore

S3Client = Any
Event = Dict[str, Any]
Context = Any
Getter = Callable[[h5py.File], Dict[str, np.ndarray]]


class NothingToDo(Exception):
    pass


class MatsScheduleException(Exception):
    pass


def get_or_raise(variable_name: str) -> str:
    if (var := os.environ.get(variable_name)) is None:
        raise EnvironmentError(
            f"{variable_name} is a required environment variable"
        )
    return var


def parse_event_message(event: Event) -> Tuple[str, str]:
    message: Dict[str, Any] = json.loads(event["Records"][0]["body"])
    object = message["object"]
    bucket = message["bucket"]
    return object, bucket


def get_partitioned_dates(datetimes: np.ndarray) -> Dict[str, np.ndarray]:
    Y, M, D = [datetimes.astype(f"M8[{x}]") for x in "YMD"]
    return {
        "year": Y.astype(int) + 1970,
        "month": (M - Y + 1).astype(int),
        "day": (D - M + 1).astype(int),
    }


def download_file(
    s3_client: S3Client,
    bucket_name: str,
    file_name: str,
    output_dir: TemporaryDirectory,
) -> Path:
    file_path = Path(f"{output_dir.name}/{file_name}")
    s3_client.download_file(bucket_name, file_name, str(file_path))
    return file_path


def get_filename(filename: str) -> str:
    return filename.removesuffix(".csv") + "_{i}.parquet"


def lambda_handler(event: Event, context: Context):
    out_bucket = get_or_raise("OUTPUT_BUCKET")
    region = os.environ.get('AWS_REGION', "eu-north-1")
    object, in_bucket = parse_event_message(event)
    tempdir = TemporaryDirectory()

    try:
        s3_client = boto3.client('s3')
        csv_path = download_file(s3_client, in_bucket, object, tempdir)
        table = csv.read_csv(csv_path)
    except Exception as err:
        msg = f"Could not get object {object} from {in_bucket}: {err}"
        raise MatsScheduleException(msg) from err

    try:
        pq.write_to_dataset(
            table=table,
            root_path=out_bucket,
            basename_template=get_filename(object),
            existing_data_behavior="overwrite_or_ignore",
            filesystem=pa.fs.S3FileSystem(
                region=region,
                request_timeout=10,
                connect_timeout=10
            ),
            version='2.6',
        )
    except Exception as err:
        msg = f"Could not process {csv_path.name}: {err}"
        raise MatsScheduleException(msg) from err

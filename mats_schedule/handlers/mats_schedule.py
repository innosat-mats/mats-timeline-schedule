
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, Tuple

import boto3
import pyarrow as pa  # type: ignore
import pyarrow.parquet as pq  # type: ignore
import pyarrow.csv as csv  # type: ignore

S3Client = Any
Event = Dict[str, Any]
Context = Any

ScheduleSchema = {
    "start_date": pa.timestamp("ms"),
    "end_date": pa.timestamp("ms"),
    "id": pa.int64(),
    "name": pa.string(),
    "version": pa.int64(),
    "standard_altitude": pa.int64(),
    "yaw_correction": pa.bool_(),
    "pointing_altitudes": pa.string(),
    "xml_file": pa.string(),
    "description_short": pa.string(),
    "description_long": pa.string(),
}


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
        table = csv.read_csv(
            csv_path,
            convert_options=csv.ConvertOptions(column_types=ScheduleSchema),
        )
        data = table.to_pandas()
        data["pointing_altitudes"] = data.apply(
            lambda s: [
                int(v) for v in s.pointing_altitudes[1:-1].split(",") if v
            ],
            axis=1,
        )
        out_table = pa.Table.from_pandas(data)
    except Exception as err:
        msg = f"Could not get object {object} from {in_bucket}: {err}"
        raise MatsScheduleException(msg) from err

    try:
        pq.write_to_dataset(
            table=out_table,
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

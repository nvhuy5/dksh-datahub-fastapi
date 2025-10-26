import io
import json
from botocore.exceptions import BotoCoreError, ClientError
from pydantic import BaseModel
from utils import log_helpers
from connections import aws_connection
from models.tracking_models import ServiceLog, LogType


# === Set up logging ===
logger = log_helpers.get_logger("Read and Write to S3")

_s3_connectors = {}


def put_object(client, bucket_name: str, object_name: str, uploading_data) -> dict:
    """Upload data (buffer or file path) to S3."""
    try:
        if isinstance(uploading_data, (io.BytesIO, io.StringIO)):
            uploading_data.seek(0)
            client.upload_fileobj(uploading_data, Bucket=bucket_name, Key=object_name)
        elif isinstance(uploading_data, str):
            client.upload_file(Filename=uploading_data, Bucket=bucket_name, Key=object_name)
        else:
            return {
                "status": "Failed",
                "error": "uploading data must be buffer or file path",
            }
        return {"status": "Success"}
    except (ClientError, BotoCoreError, TypeError) as e:
        return {"status": "Failed", "error": str(e)}


def get_object(client, bucket_name: str, object_name: str) -> io.BytesIO | None:
    """Download an object from S3 into BytesIO buffer."""
    try:
        response = client.get_object(Bucket=bucket_name, Key=object_name)
        data = response["Body"].read()
        buffer = io.BytesIO(data)
        return buffer
    except (ClientError, BotoCoreError):
        return None


def copy_object_between_buckets(
    source_bucket: str, source_key: str, dest_bucket: str, dest_key: str
) -> dict:
    """Copy object between S3 buckets."""
    try:
        logger.info(f"Copying {source_bucket}/{source_key} → {dest_bucket}/{dest_key}")
        if source_bucket not in _s3_connectors:
            _s3_connectors[source_bucket] = aws_connection.S3Connector(bucket_name=source_bucket)
        client = _s3_connectors[source_bucket].client
        client.copy_object(
            CopySource={"Bucket": source_bucket, "Key": source_key},
            Bucket=dest_bucket,
            Key=dest_key,
        )
        return {
            "status": "Success",
            "source": {"bucket": source_bucket, "key": source_key},
            "destination": {"bucket": dest_bucket, "key": dest_key},
        }
    except (ClientError, BotoCoreError) as e:
        return {
            "status": "Failed",
            "error": str(e),
            "source": {"bucket": source_bucket, "key": source_key},
            "destination": {"bucket": dest_bucket, "key": dest_key},
        }


def object_exists(
    client, bucket_name: str, object_name: str
) -> tuple[bool, dict | None]:
    """Check if object exists and return metadata."""
    try:
        response = client.head_object(Bucket=bucket_name, Key=object_name)
        return True, response
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False, None
        return False, None


def any_json_in_s3_prefix(bucket_name: str, s3_key_prefix: str) -> bool:
    """Check if any .json file exists under the given prefix."""
    if bucket_name not in _s3_connectors:
        _s3_connectors[bucket_name] = aws_connection.S3Connector(bucket_name=bucket_name)
    client = _s3_connectors[bucket_name].client
    paginator = client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=s3_key_prefix)

    for page in page_iterator:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json"):
                return True
    return False


def write_json_to_s3(
    json_data,
    bucket_name: str,
    s3_key_prefix: str = "",
) -> dict:
    """
    Write JSON data to an S3 bucket.
    """
    # Prepare S3 connector
    if bucket_name not in _s3_connectors:
        _s3_connectors[bucket_name] = aws_connection.S3Connector(bucket_name=bucket_name)
    s3_connector = _s3_connectors[bucket_name]
    client, bucket = s3_connector.client, s3_connector.bucket_name

    try:
        # Determine payload preserving .data logic
        if isinstance(json_data, BaseModel):
            output_attr = getattr(json_data, "data", None)
            if output_attr is not None:
                payload = (
                    output_attr.model_dump()
                    if isinstance(output_attr, BaseModel)
                    else output_attr
                )
            else:
                payload = json_data.model_dump()
        else:
            payload = json_data

        buffer = io.BytesIO(json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8"))
        buffer.seek(0)

        upload_result = put_object(client, bucket, s3_key_prefix, buffer)
        if upload_result.get("status") == "Failed":
            logger.error(
                f"Failed to upload object to S3: bucket={bucket} object={s3_key_prefix} error={upload_result.get('error')}",
                extra={
                    "service": ServiceLog.FILE_STORAGE,
                    "log_type": LogType.ERROR,
                    "data": {
                        "s3_key_prefix": s3_key_prefix
                    },
                },
            )
            return {
                "status": "Failed",
                "error": upload_result.get("error"),
                "s3_key_prefix": s3_key_prefix,
            }

        logger.info(
            f"Uploaded JSON to S3: s3://{bucket}/{s3_key_prefix}",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "log_type": LogType.TASK,
                "data": {
                    "s3_key_prefix": s3_key_prefix
                },
            },
        )
        return {
            "status": "Success",
            "error": None,
            "s3_key_prefix": s3_key_prefix,
        }

    except Exception as e:
        logger.exception("Upload failed")
        return {"status": "Failed", "error": str(e), "s3_key_prefix": s3_key_prefix}


def read_json_from_s3(bucket_name: str, object_name: str) -> dict | None:
    """Read and parse JSON object from S3."""
    try:
        if bucket_name not in _s3_connectors:
            _s3_connectors[bucket_name] = aws_connection.S3Connector(bucket_name=bucket_name)
        client = _s3_connectors[bucket_name].client
        # Get the object
        buffer = get_object(client, bucket_name, object_name)
        if not buffer:
            return None
        # Parse JSON
        content = buffer.read().decode("utf-8")
        data = json.loads(content)
        return data
    except Exception as e:
        logger.error(f"Failed to read JSON from S3: {e}", exc_info=True)
        return None


def list_objects_with_prefix(bucket_name: str, prefix: str) -> list:
    """List all object keys under a given prefix."""
    try:
        if bucket_name not in _s3_connectors:
            _s3_connectors[bucket_name] = aws_connection.S3Connector(bucket_name=bucket_name)
        client = _s3_connectors[bucket_name].client
        keys = []
        paginator = client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        for page in page_iterator:
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys
    except Exception:
        return []


def select_latest_rerun(keys: list[str], base_filename: str) -> str | None:
    """
    Select the latest rerun JSON file from an S3 key list.
    Prefer the highest rerun number, fallback to the base file if no rerun exists.

    Example:
        base_filename = "DN800018251920240708123641"
    """
    reruns = [
        (int(k.split("_rerun_")[-1].split(".json")[0]), k)
        for k in keys if f"{base_filename}_rerun_" in k
    ]
    if reruns:
        return max(reruns, key=lambda x: x[0])[1]
    for k in keys:
        if f"{base_filename}.json" in k and "_rerun_" not in k:
            return k
    return None

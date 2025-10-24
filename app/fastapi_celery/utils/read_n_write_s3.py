# Standard Library Imports
import io
import json
import traceback

# Third-Party Imports
from botocore.exceptions import BotoCoreError, ClientError
from pydantic import BaseModel

# Local Application Imports
from utils import log_helpers
from connections import aws_connection
from models.tracking_models import ServiceLog, LogType
from typing import Optional


# === Set up logging ===
logger = log_helpers.get_logger("Read and Write to S3")

_s3_connectors = {}


# === Upload to S3 ===
def put_object(client, bucket_name: str, object_name: str, uploading_data) -> dict:
    """
    Upload data to an S3 bucket.

    Supports uploading from a buffer (BytesIO/StringIO) or a file path.

    Args:
        client: AWS S3 client.
        bucket_name (str): Target S3 bucket name.
        object_name (str): S3 object key.
        uploading_data: Data to upload (buffer or file path).

    Returns:
        dict: Status dictionary with "status" ("Success" or "Failed") and optional "error".

    Raises:
        ClientError: If S3 upload fails.
        BotoCoreError: If AWS client encounters an error.
        TypeError: If uploading_data is invalid.
    """
    try:
        if isinstance(uploading_data, (io.BytesIO, io.StringIO)):
            uploading_data.seek(0)
            client.upload_fileobj(uploading_data, Bucket=bucket_name, Key=object_name)
            return {"status": "Success"}
        elif isinstance(uploading_data, str):
            client.upload_file(Filename=uploading_data, Bucket=bucket_name, Key=object_name)
            return {"status": "Success"}
        else:
            msg = "uploading_data must be either a buffer-like object or a file path"
            return {"status": "Failed", "error": msg}

    except (ClientError, BotoCoreError, TypeError) as e:
        return {"status": "Failed", "error": str(e)}


# === Download from S3 ===
def get_object(client, bucket_name: str, object_name: str) -> io.BytesIO | None:
    """
    Download an object from an S3 bucket.

    Args:
        client: AWS S3 client.
        bucket_name (str): S3 bucket name.
        object_name (str): S3 object key.

    Returns:
        io.BytesIO | None: Buffer with object data, or None if download fails.

    Raises:
        ClientError: If S3 object retrieval fails.
        BotoCoreError: If AWS client encounters an error.
    """
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
    """
    Copy an object between S3 buckets.

    Args:
        source_bucket (str): Source S3 bucket name.
        source_key (str): Source S3 object key.
        dest_bucket (str): Destination S3 bucket name.
        dest_key (str): Destination S3 object key.

    Returns:
        dict: Status dictionary with "status" ("Success" or "Failed"), source, destination, and optional "error".

    Raises:
        ClientError: If S3 copy operation fails.
        BotoCoreError: If AWS client encounters an error.
    """
    try:
        logger.info(
            f"Start copying from {source_bucket}/{source_key} to {dest_bucket}/{dest_key}"
        )
        s3_connector = aws_connection.S3Connector(bucket_name=source_bucket)
        client = s3_connector.client
        copy_source = {"Bucket": source_bucket, "Key": source_key}

        client.copy_object(CopySource=copy_source, Bucket=dest_bucket, Key=dest_key)

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


# === Check if object exists or get metadata ===
def object_exists(
    client, bucket_name: str, object_name: str
) -> tuple[bool, dict | None]:
    """
    Check if an object exists in an S3 bucket and retrieve its metadata.

    Args:
        client: AWS S3 client.
        bucket_name (str): S3 bucket name.
        object_name (str): S3 object key.

    Returns:
        tuple[bool, dict | None]: (True, metadata) if object exists, (False, None) if not or on error.

    Raises:
        ClientError: If S3 metadata retrieval fails (except 404).
    """
    try:
        response = client.head_object(Bucket=bucket_name, Key=object_name)
        return True, response
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False, None
        return False, None


def any_json_in_s3_prefix(bucket_name: str, prefix: str) -> bool:
    """
    Check if any .json file exists under the given prefix in S3.

    Args:
        bucket_name (str): The S3 bucket name.
        prefix (str): The S3 prefix path (like a folder).

    Returns:
        bool: True if any .json file exists, False otherwise.
    """
    if bucket_name not in _s3_connectors:
        _s3_connectors[bucket_name] = aws_connection.S3Connector(
            bucket_name=bucket_name
        )
        logger.info(f"Created new S3Connector for bucket: {bucket_name}")
    else:
        logger.info(f"Reusing existing S3Connector for bucket: {bucket_name}")

    client = _s3_connectors[bucket_name].client
    paginator = client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    for page in page_iterator:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json"):
                return True
    return False


def write_json_to_s3(
    json_data,
    file_record: dict,
    bucket_name: str,
    s3_key_prefix: str = "",
    rerun_attempt: Optional[int] = None,
) -> dict:
    """
    Write JSON data to an S3 bucket.

    Args:
        json_data: JSON-serializable data to upload.
        file_record (dict): File metadata with "file_name" or "object_name".
        bucket_name (str): Target S3 bucket name.
        s3_key_prefix (str, optional): Prefix for S3 object key. Defaults to "".

    Returns:
        dict: Status dictionary with "status" ("Success" or "Failed"), json_data, file_info,
        and optional convert_file_info or error.

    Raises:
        Exception: If S3 connection or JSON upload fails.
    """
    try:
        # Determine extension
        ext = ".json"
        base_name = file_record.get("file_name", "unnamed").rsplit(".", 1)[0]

        # Append rerun_attempt if present
        if rerun_attempt is not None:
            object_name = f"{base_name}_rerun_{rerun_attempt}{ext}"
        else:
            object_name = f"{base_name}{ext}"

        # Apply prefix if given
        if file_record.get("s3_key_prefix", None):
            object_name = f"{file_record['s3_key_prefix'].rstrip('/')}/{base_name}_{file_record['current_time']}{ext}"
            # keep semantics similar to previous impl: remove to avoid confusion upstream
            try:
                del file_record["s3_key_prefix"]
            except Exception:
                pass
        elif s3_key_prefix:
            object_name = f"{s3_key_prefix.rstrip('/')}/{object_name}"

        if bucket_name not in _s3_connectors:
            _s3_connectors[bucket_name] = aws_connection.S3Connector(
                bucket_name=bucket_name
            )
            logger.info(f"Create new S3Connector for bucket: {bucket_name}")
        else:
            logger.info(f"Reusing existing S3Connector for bucket : {bucket_name}")

        s3_connector = _s3_connectors[bucket_name]
        client = s3_connector.client
        bucket = s3_connector.bucket_name
    except Exception as e:
        logger.exception("Failed to prepare S3 connector or object name")
        return {
            "status": "Failed",
            "error": "S3 connection failed",
            "file_info": file_record,
        }

    try:
        buffer = io.BytesIO()
        # --- FIX: choose payload robustly ---
        if isinstance(json_data, BaseModel):
            # If StepOutput or other BaseModel and .output exists and not None -> prefer it.
            # Otherwise fallback to whole model (so NOT_DEFINED with output=None is preserved).
            try:
                output_attr = getattr(json_data, "output", None)
                if output_attr is not None:
                    # if output is BaseModel -> dump; if primitive/dict -> use directly
                    if isinstance(output_attr, BaseModel):
                        payload = output_attr.model_dump()
                    else:
                        payload = output_attr
                else:
                    # output is None -> serialize entire StepOutput model (keeps step_status, messages)
                    payload = json_data.model_dump()
            except Exception:
                # defensive fallback: serialize whole model
                payload = json_data.model_dump()
        else:
            payload = json_data

        buffer.write(
            json.dumps(
                payload,
                ensure_ascii=False,
                default=str,
            ).encode("utf-8")
        )
        buffer.seek(0)

        upload_result = put_object(client, bucket, object_name, buffer)
        if upload_result.get("status") == "Failed":
            error_message = upload_result.get("error", "Unknown error")
            # Add detailed log to help debugging missing object cases
            logger.error(
                f"Failed to upload object to S3. bucket={bucket} object={object_name} error={error_message}",
                extra={"service": ServiceLog.FILE_STORAGE, "log_type": LogType.ERROR, "data": file_record},
            )
            return {
                "status": "Failed",
                "error": error_message,
                "file_info": file_record,
            }

        # Success
        logger.info(
            f"Uploaded JSON to S3: s3://{bucket}/{object_name}",
            extra={"service": ServiceLog.FILE_STORAGE, "log_type": LogType.TASK, "data": file_record},
        )
        return {
            "json_data": json_data,
            "file_info": file_record,
            "convert_file_info": {
                "dest_bucket": bucket,
                "dest_object_name": object_name,
            },
            "status": "Success",
        }

    except Exception as e:
        logger.exception(f"Upload failed: {traceback.format_exc()}")
        return {"status": "Failed", "error": f"Upload failed: {str(e)}", "file_info": file_record}


def read_json_from_s3(bucket_name: str, object_name: str) -> dict | None:
    """
    Read a JSON object from S3 and parse it.

    Args:
        bucket_name (str): S3 bucket name.
        object_name (str): S3 object key.

    Returns:
        dict | None: Parsed JSON content, or None if not found or error.
    """
    try:
        # Get or create S3Connector
        if bucket_name not in _s3_connectors:
            _s3_connectors[bucket_name] = aws_connection.S3Connector(
                bucket_name=bucket_name
            )
            logger.info(f"Created new S3Connector for bucket: {bucket_name}")
        else:
            logger.info(f"Reusing existing S3Connector for bucket: {bucket_name}")

        s3_connector = _s3_connectors[bucket_name]
        client = s3_connector.client
        bucket = s3_connector.bucket_name

    except Exception:
        return None

    try:
        # Get the object
        buffer = get_object(client, bucket_name=bucket, object_name=object_name)
        if buffer is None:
            return None

        # Parse JSON
        content = buffer.read().decode("utf-8")
        data = json.loads(content)
        return data

    except Exception:
        return None


def list_objects_with_prefix(bucket_name: str, prefix: str) -> list:
    """
    List all object keys in bucket with given prefix.

    Args:
        bucket_name (str): S3 bucket_name
        prefix (str): S3_prefix_key
    Returns:
        list: List of keys (strings) if found, empty if none or error.
    """
    try:
        if bucket_name not in _s3_connectors:
            _s3_connectors[bucket_name] = aws_connection.S3Connector(
                bucket_name=bucket_name
            )
            logger.info(f"Created new S3Connector for bucket: {bucket_name}")
        else:
            logger.info(f"Reusing existing S3Connector for bucket: {bucket_name}")

        client = _s3_connectors[bucket_name].client
        paginator = client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

        keys = []
        for page in page_iterator:
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys

    except Exception:
        return []

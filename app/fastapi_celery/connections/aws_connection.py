import json
from utils import log_helpers
from typing import Optional
import traceback
import boto3
from botocore.exceptions import ClientError
import config_loader
from models.tracking_models import ServiceLog, LogType

# === Set up logging ===
logger = log_helpers.get_logger("AWS Connection")

# === Environment setup ===
aws_region = config_loader.get_env_variable("s3_buckets", "default_region")

# === S3 Connector using boto3 ===
class S3Connector:
    """AWS S3 Connector using boto3 for bucket operations.

    Initializes an S3 client, ensures the specified bucket exists, and provides methods
    for interacting with S3. Logs progress and errors.
    """

    def __init__(self, bucket_name: str, region_name: str = None):
        """Initialize S3 connector with a bucket and region.

        Args:
            bucket_name (str): Name of the S3 bucket to connect to.
            region_name (str, optional): AWS region name. Defaults to 'ap-southeast-1' if not specified.
        """

        self.bucket_name = bucket_name.strip()
        self.region_name = (
            region_name
            or config_loader.get_env_variable("AWS_REGION", "ap-southeast-1")
        ).strip()

        # Initialize boto3 client with region and optional credentials
        self.client = boto3.client("s3", region_name=self.region_name)

        # Check if bucket exists or try to create it
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self) -> None:  # pragma: no cover  # NOSONAR
        """Ensure the S3 bucket exists, create it if it doesn't.

        Checks if the bucket exists using head_bucket. If it doesn't exist (404),
        creates a new bucket. Logs progress and errors.

        Raises:
            ClientError: If bucket check fails for reasons other than 404.
        """
        try:
            self.client.head_bucket(Bucket=self.bucket_name)
            logger.info(
                f"Bucket '{self.bucket_name}' already exists.",
                extra={
                    "service": ServiceLog.FILE_STORAGE,
                    "log_type": LogType.ACCESS,
                },
            )
        except ClientError as e:
            error_code = int(e.response["Error"]["Code"])
            if error_code == 404:
                logger.warning(
                    f"Bucket '{self.bucket_name}' does not exist. Creating...",
                    extra={
                        "service": ServiceLog.FILE_STORAGE,
                        "log_type": LogType.ERROR,
                    },
                )
                self._create_bucket()
            else:
                full_tb = traceback.format_exception(type(e), e, e.__traceback__)
                logger.error(
                    f"Error checking bucket '{self.bucket_name}': {type(e).__name__} - {e}\n",
                    extra={
                        "service": ServiceLog.FILE_STORAGE,
                        "log_type": LogType.ERROR,
                        "traceback": full_tb,
                    },
                )
                raise

    def _create_bucket(self) -> None:  # pragma: no cover  # NOSONAR
        """Create the S3 bucket in the specified region.

        Creates a bucket with region-specific configuration. For 'us-east-1',
        skips LocationConstraint. Logs success or errors.

        Raises:
            ClientError: If bucket creation fails.
        """
        try:
            if self.region_name == "us-east-1":
                # us-east-1 does not support LocationConstraint
                self.client.create_bucket(Bucket=self.bucket_name)
            else:
                self.client.create_bucket(
                    Bucket=self.bucket_name,
                    CreateBucketConfiguration={"LocationConstraint": self.region_name},
                )
            logger.info(
                f"Bucket '{self.bucket_name}' created successfully.",
                extra={
                    "service": ServiceLog.FILE_STORAGE,
                    "log_type": LogType.ACCESS,
                },
            )
        except ClientError as e:
            full_tb = traceback.format_exception(type(e), e, e.__traceback__)
            logger.error(
                f"Error creating bucket '{self.bucket_name}': {type(e).__name__} - {e}\n",
                extra={
                    "service": ServiceLog.FILE_STORAGE,
                    "log_type": LogType.ERROR,
                    "traceback": full_tb,
                },
            )
            raise


# === Retrieve Secrets using boto3 ===
class AWSSecretsManager:
    """AWS Secrets Manager Connector using boto3 for retrieving secrets.

    Initializes a Secrets Manager client and provides methods to retrieve secrets.
    Logs errors during secret retrieval.
    """

    def __init__(self, region_name: str = None):
        """Initialize Secrets Manager connector with a region.

        Args:
            region_name (str, optional): AWS region name. Defaults to 'ap-southeast-1' if not specified.
        """
        self.region_name = region_name or config_loader.get_env_variable(
            "AWS_REGION", "ap-southeast-1"
        )

        # Initialize boto3 client with region and optional credentials
        self.client = boto3.client("secretsmanager", region_name=self.region_name)

    def get_secret(self, secret_name: str) -> Optional[dict]:
        """Retrieve a secret from AWS Secrets Manager.

        Fetches a secret by name, parses it as JSON, and returns it as a dictionary.
        Logs errors if retrieval fails.

        Args:
            secret_name (str): Name of the secret to retrieve.

        Returns:
            Optional[dict]: Secret value as a dictionary, or None if retrieval fails.
        """
        try:
            response = self.client.get_secret_value(SecretId=secret_name)

            # Secret is either string or binary
            if "SecretString" in response:
                return json.loads(response["SecretString"])
            else:
                return json.loads(response["SecretBinary"].decode("utf-8"))
        except ClientError as e:  # pragma: no cover  # NOSONAR
            error_code = e.response["Error"]["Code"]
            if error_code == "ResourceNotFoundException":
                logger.error(f"Secret not found. - error code: {error_code}")
            else:
                logger.error(f"ClientError retrieving secret '{secret_name}': {error_code} - {e}")
        except Exception as e:
            logger.error(f"Error retrieving secret: {e} - {traceback.format_exc()}")

        return None

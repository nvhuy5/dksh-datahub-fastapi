import io
import json
import logging
import pytest
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
from fastapi_celery.connections.aws_connection import S3Connector, AWSSecretsManager

# === S3Connector Tests ===

@patch("boto3.client")
def test_s3connector_head_bucket_exists(mock_boto_client):
    """Test that S3Connector does not create bucket if it already exists."""
    mock_client = mock_boto_client.return_value
    mock_client.head_bucket.return_value = {}

    connector = S3Connector(bucket_name="existing-bucket")
    assert connector.bucket_name == "existing-bucket"
    mock_client.head_bucket.assert_called_once_with(Bucket="existing-bucket")

@patch("boto3.client")
def test_s3connector_create_bucket_if_not_exists(mock_boto_client):
    """Test that S3Connector creates bucket if head_bucket returns 404."""
    mock_client = mock_boto_client.return_value
    mock_client.head_bucket.side_effect = ClientError(
        {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadBucket"
    )
    mock_client.create_bucket.return_value = {}

    connector = S3Connector(bucket_name="new-bucket")
    mock_client.create_bucket.assert_called_once()

@patch("boto3.client")
def test_s3connector_create_bucket_raises_client_error(mock_boto_client):
    """Test that S3Connector raises ClientError for non-404 head_bucket errors."""
    mock_client = mock_boto_client.return_value
    mock_client.head_bucket.side_effect = ClientError(
        {"Error": {"Code": "500", "Message": "InternalError"}}, "HeadBucket"
    )

    with pytest.raises(ClientError):
        S3Connector(bucket_name="fail-bucket")

# === AWSSecretsManager Tests ===

@patch("boto3.client")
def test_aws_secrets_manager_get_secret_string(mock_boto_client):
    """Test AWSSecretsManager returns secret correctly when SecretString is present."""
    secret_value = {"username": "admin", "password": "123"}
    mock_client = mock_boto_client.return_value
    mock_client.get_secret_value.return_value = {"SecretString": json.dumps(secret_value)}

    manager = AWSSecretsManager()
    result = manager.get_secret("my-secret")
    assert result == secret_value

@patch("boto3.client")
def test_aws_secrets_manager_get_secret_binary(mock_boto_client):
    """Test AWSSecretsManager returns secret correctly when SecretBinary is present."""
    secret_value = {"key": "value"}
    mock_client = mock_boto_client.return_value
    mock_client.get_secret_value.return_value = {"SecretBinary": json.dumps(secret_value).encode("utf-8")}

    manager = AWSSecretsManager()
    result = manager.get_secret("binary-secret")
    assert result == secret_value

@patch("boto3.client")
def test_aws_secrets_manager_secret_not_found_returns_none(mock_boto_client):
    """AWSSecretsManager returns None when secret is not found."""
    mock_client = mock_boto_client.return_value
    mock_client.get_secret_value.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "Not found"}},
        "GetSecret",
    )

    manager = AWSSecretsManager()
    result = manager.get_secret("missing-secret")
    assert result is None


@patch("boto3.client")
def test_aws_secrets_manager_generic_exception_returns_none(mock_boto_client):
    """AWSSecretsManager returns None when generic exception occurs."""
    mock_client = mock_boto_client.return_value
    mock_client.get_secret_value.side_effect = Exception("Unexpected error")

    manager = AWSSecretsManager()
    result = manager.get_secret("any-secret")
    assert result is None

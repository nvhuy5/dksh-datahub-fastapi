import pytest
from unittest.mock import patch
from types import SimpleNamespace
from fastapi_celery.processors.workflow_processors import write_raw_to_s3
from fastapi_celery.models.class_models import DocumentType, StepOutput, StatusEnum


# ============================================================
# TEST: get_source_bucket
# ============================================================

@patch("fastapi_celery.processors.workflow_processors.write_raw_to_s3.config_loader.get_config_value")
def test_get_source_bucket_valid_tw(mock_get_config):
    mock_get_config.return_value = "s3-tw-bucket"
    result = write_raw_to_s3.get_source_bucket(DocumentType.MASTER_DATA, "DKSH_TW")
    assert result == "s3-tw-bucket"
    mock_get_config.assert_called_once_with("s3_buckets", "datahub_s3_raw_data_tw")


@patch("fastapi_celery.processors.workflow_processors.write_raw_to_s3.config_loader.get_config_value")
def test_get_source_bucket_invalid_project(mock_get_config):
    mock_get_config.return_value = None
    result = write_raw_to_s3.get_source_bucket(DocumentType.MASTER_DATA, "UNKNOWN")
    assert result is None
    mock_get_config.assert_not_called()


# ============================================================
# TEST: get_destination_bucket
# ============================================================

@patch("fastapi_celery.processors.workflow_processors.write_raw_to_s3.config_loader.get_config_value")
def test_get_destination_bucket_sap_masterdata(mock_get_config):
    mock_get_config.return_value = "sap-bucket"
    result = write_raw_to_s3.get_destination_bucket("DKSH_TW", True)
    assert result == "sap-bucket"
    mock_get_config.assert_called_once_with("s3_buckets", "sap_masterdata_bucket")


@patch("fastapi_celery.processors.workflow_processors.write_raw_to_s3.config_loader.get_config_value")
def test_get_destination_bucket_invalid_project(mock_get_config):
    mock_get_config.return_value = None
    result = write_raw_to_s3.get_destination_bucket("UNKNOWN", False)
    assert result is None
    mock_get_config.assert_not_called()


# ============================================================
# TEST: write_raw_to_s3 SUCCESS
# ============================================================

@patch("fastapi_celery.processors.workflow_processors.write_raw_to_s3.read_n_write_s3.list_objects_with_prefix")
@patch("fastapi_celery.processors.workflow_processors.write_raw_to_s3.read_n_write_s3.copy_object_between_buckets")
@patch("fastapi_celery.processors.workflow_processors.write_raw_to_s3.get_source_bucket")
@patch("fastapi_celery.processors.workflow_processors.write_raw_to_s3.get_destination_bucket")
def test_write_raw_to_s3_success(mock_get_dest, mock_get_src, mock_copy, mock_list):
    mock_get_src.return_value = "src-bucket"
    mock_get_dest.return_value = "dest-bucket"
    mock_copy.return_value = {"result": "ok"}
    mock_list.return_value = ["versioning/sample/001/", "versioning/sample/002/"]

    fake_self = SimpleNamespace(
        document_type=DocumentType.MASTER_DATA,
        tracking_model=SimpleNamespace(project_name="DKSH_TW", sap_masterdata=True)
    )

    result = write_raw_to_s3.write_raw_to_s3(fake_self, "data/sample.xlsx")

    assert result.__class__.__name__ == "StepOutput"
    assert result.step_status.value == StatusEnum.SUCCESS.value
    assert result.output == {"result": "ok"}
    assert mock_copy.call_count == 2
    mock_list.assert_called_once()


# ============================================================
# TEST: write_raw_to_s3 EXCEPTION
# ============================================================

@patch("fastapi_celery.processors.workflow_processors.write_raw_to_s3.read_n_write_s3.copy_object_between_buckets", side_effect=Exception("S3 error"))
@patch("fastapi_celery.processors.workflow_processors.write_raw_to_s3.get_source_bucket", return_value="src-bucket")
@patch("fastapi_celery.processors.workflow_processors.write_raw_to_s3.get_destination_bucket", return_value="dest-bucket")
def test_write_raw_to_s3_exception(mock_get_dest, mock_get_src, mock_copy):
    fake_self = SimpleNamespace(
        document_type=DocumentType.MASTER_DATA,
        tracking_model=SimpleNamespace(project_name="DKSH_VN", sap_masterdata=False)
    )

    result = write_raw_to_s3.write_raw_to_s3(fake_self, "data/error.xlsx")

    assert result.__class__.__name__ == "StepOutput"
    assert result.step_status.value == StatusEnum.FAILED.value
    assert result.output is None
    assert "Exception in write_raw_to_s3" in result.step_failure_message[0]

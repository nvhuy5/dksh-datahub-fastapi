import pytest
import types
import pandas as pd
from unittest.mock import patch, AsyncMock

from fastapi_celery.processors.workflow_processors.master_validation import (
    MasterValidation,
    masterdata_header_validation,
    masterdata_data_validation,
)
from fastapi_celery.models.class_models import MasterDataParsed, StatusEnum, StepOutput


# ====== Fixtures ======

@pytest.fixture
def fake_tracking_model():
    class FakeTrackingModel:
        def __init__(self, file_path: str):
            self.file_path = file_path
            self.project_name = "TEST_PROJECT"
            self.bucket_name = "mock-bucket"
            self.request_id = "REQ-001"

    return FakeTrackingModel


@pytest.fixture
def masterdata_json():
    data = {
        "ID": ["1", "2"],
        "Name": ["Alice", "Bob"],
        "Date": ["20250101", "20250201"]
    }
    headers = ["ID", "Name", "Date"]
    return MasterDataParsed(
        original_file_path="dummy_path",
        headers=headers,
        document_type="master_data",
        items=data,
        step_status=StatusEnum.SUCCESS,
        messages=[],
        capacity="1KB",
    )


@pytest.fixture
def master_validation(masterdata_json, fake_tracking_model):
    tracking_model = fake_tracking_model("tests/samples/0808fake_xlsx.xlsx")
    mv = MasterValidation(masterdata_json, tracking_model)
    return mv


# ====== Header validation tests ======

def test_header_validation_success(master_validation):
    header_ref = [
        {"name": "ID", "posidx": 0},
        {"name": "Name", "posidx": 1},
        {"name": "Date", "posidx": 2},
    ]
    result = master_validation.header_validation(header_ref)
    assert result.step_status == StatusEnum.SUCCESS
    assert result.messages is None


def test_header_validation_fail(master_validation):
    header_ref = [
        {"name": "ID", "posidx": 0},
        {"name": "FullName", "posidx": 1},
        {"name": "Date", "posidx": 2},
    ]
    result = master_validation.header_validation(header_ref)
    assert result.step_status == StatusEnum.FAILED
    assert any("Mismatch at position" in m for m in result.messages)


# ====== Data validation tests ======

def test_data_validation_success(master_validation):
    data_ref = [
        {"name": "ID", "datatype": "int", "nullable": False},
        {"name": "Name", "datatype": "string", "nullable": False, "maxlength": 10},
        {"name": "Date", "datatype": "timestamp", "nullable": False},
    ]
    result = master_validation.data_validation(data_ref)
    assert result.step_status == StatusEnum.SUCCESS


def test_data_validation_missing_column(master_validation):
    data_ref = [
        {"name": "ID", "datatype": "int"},
        {"name": "NonExist", "datatype": "string"},
    ]
    result = master_validation.data_validation(data_ref)
    assert result.step_status == StatusEnum.FAILED
    assert "Missing column" in result.messages[0]


def test_data_validation_invalid_type(master_validation):
    master_validation.masterdata["ID"] = ["1", "x"]
    data_ref = [{"name": "ID", "datatype": "int"}]
    result = master_validation.data_validation(data_ref)
    assert result.step_status == StatusEnum.FAILED
    assert "invalid value" in result.messages[0]


# ====== Async masterdata header/data validation ======

class DummySelf:
    file_record = {"file_name": "dummy.txt"}
    tracking_model = types.SimpleNamespace(file_path="dummy_path")


@pytest.mark.asyncio
async def test_masterdata_header_validation_success(masterdata_json):
    input_data = StepOutput(
        output=masterdata_json,
        step_status=StatusEnum.SUCCESS,
        step_failure_message=None,
    )
    dummy_self = DummySelf()

    async_mock = AsyncMock(
        return_value=[
            {"name": "ID", "posidx": 0},
            {"name": "Name", "posidx": 1},
            {"name": "Date", "posidx": 2},
        ]
    )

    with patch(
        "fastapi_celery.processors.workflow_processors.master_validation.BEConnector"
    ) as mock_conn, patch(
        "fastapi_celery.processors.workflow_processors.master_validation.MasterValidation"
    ) as mock_mv:
        mock_conn.return_value.get = async_mock
        instance = mock_mv.return_value
        instance.header_validation.return_value = masterdata_json

        result = await masterdata_header_validation(dummy_self, input_data)
        assert result.step_status == StatusEnum.SUCCESS


@pytest.mark.asyncio
async def test_masterdata_header_validation_fail(masterdata_json):
    input_data = StepOutput(
        output=masterdata_json,
        step_status=StatusEnum.SUCCESS,
        step_failure_message=None,
    )
    dummy_self = DummySelf()

    async_mock = AsyncMock(
        return_value=[
            {"name": "ID", "posidx": 0},
            {"name": "FullName", "posidx": 1},
            {"name": "Date", "posidx": 2},
        ]
    )

    with patch(
        "fastapi_celery.processors.workflow_processors.master_validation.BEConnector"
    ) as mock_conn, patch(
        "fastapi_celery.processors.workflow_processors.master_validation.MasterValidation"
    ) as mock_mv:
        mock_conn.return_value.get = async_mock
        instance = mock_mv.return_value
        instance.header_validation.return_value = masterdata_json.model_copy(
            update={"step_status": StatusEnum.FAILED}
        )

        result = await masterdata_header_validation(dummy_self, input_data)
        assert result.step_status == StatusEnum.FAILED


@pytest.mark.asyncio
async def test_masterdata_data_validation_success(masterdata_json):
    input_data = StepOutput(
        output=masterdata_json,
        step_status=StatusEnum.SUCCESS,
        step_failure_message=None,
    )
    dummy_self = DummySelf()

    async_mock = AsyncMock(
        return_value=[
            {"name": "ID", "datatype": "int", "nullable": False},
            {"name": "Name", "datatype": "string", "nullable": False, "maxlength": 10},
            {"name": "Date", "datatype": "timestamp", "nullable": False},
        ]
    )

    with patch(
        "fastapi_celery.processors.workflow_processors.master_validation.BEConnector"
    ) as mock_conn, patch(
        "fastapi_celery.processors.workflow_processors.master_validation.MasterValidation"
    ) as mock_mv:
        mock_conn.return_value.get = async_mock
        instance = mock_mv.return_value
        instance.data_validation.return_value = masterdata_json

        result = await masterdata_data_validation(dummy_self, input_data)
        assert result.step_status == StatusEnum.SUCCESS


@pytest.mark.asyncio
async def test_masterdata_data_validation_fail(masterdata_json):
    masterdata_json.items["ID"] = ["1", "x"]
    input_data = StepOutput(
        output=masterdata_json,
        step_status=StatusEnum.SUCCESS,
        step_failure_message=None,
    )
    dummy_self = DummySelf()

    async_mock = AsyncMock(
        return_value=[{"name": "ID", "datatype": "int", "nullable": False}]
    )

    with patch(
        "fastapi_celery.processors.workflow_processors.master_validation.BEConnector"
    ) as mock_conn, patch(
        "fastapi_celery.processors.workflow_processors.master_validation.MasterValidation"
    ) as mock_mv:
        mock_conn.return_value.get = async_mock
        instance = mock_mv.return_value
        instance.data_validation.return_value = masterdata_json.model_copy(
            update={"step_status": StatusEnum.FAILED}
        )

        result = await masterdata_data_validation(dummy_self, input_data)
        assert result.step_status == StatusEnum.FAILED

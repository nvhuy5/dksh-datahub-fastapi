import io
import builtins
import pytest
from unittest.mock import MagicMock, patch

from fastapi_celery.models.tracking_models import TrackingModel
from fastapi_celery.models.class_models import PODataParsed, SourceType, StatusEnum
from fastapi_celery.processors.helpers.txt_helper import TxtHelper
from models.class_models import PODataParsed


@pytest.fixture
def dummy_tracking_model(tmp_path):
    return TrackingModel(
        request_id="req-001",
        file_path=str(tmp_path / "dummy.txt"),
        file_name="dummy.txt",
        file_extension=".txt",
    )


# ==== Test extract_text() ====

def test_extract_text_s3_mode(monkeypatch, dummy_tracking_model):
    """Should extract text from S3 source using object_buffer"""
    mock_processor = MagicMock()
    mock_processor.source = "s3"
    mock_processor._get_file_capacity.return_value = "2 KB"
    mock_processor._get_document_type.return_value = "order"
    mock_processor.object_buffer = io.BytesIO(b"hello\nworld")

    monkeypatch.setattr("utils.ext_extraction.FileExtensionProcessor", lambda **_: mock_processor)

    helper = TxtHelper(dummy_tracking_model, source=SourceType.S3)
    result = helper.extract_text()

    assert result == "hello\nworld"
    assert helper.capacity == "2 KB"
    assert helper.document_type == "order"
    mock_processor._get_file_capacity.assert_called_once()
    mock_processor._get_document_type.assert_called_once()


def test_extract_text_local_mode(monkeypatch, tmp_path, dummy_tracking_model):
    """Should extract text from local file correctly"""
    file_path = tmp_path / "local.txt"
    file_path.write_text("local mode test", encoding="utf-8")

    mock_processor = MagicMock()
    mock_processor.source = "local"
    mock_processor.file_path = str(file_path)
    mock_processor._get_file_capacity.return_value = "1 KB"
    mock_processor._get_document_type.return_value = "master_data"

    monkeypatch.setattr("utils.ext_extraction.FileExtensionProcessor", lambda **_: mock_processor)

    helper = TxtHelper(dummy_tracking_model, source=SourceType.LOCAL)
    result = helper.extract_text()

    assert result == "local mode test"
    assert helper.capacity == "1 KB"
    assert helper.document_type == "master_data"


# ==== Test parse_file_to_json() ====

def test_parse_file_to_json_parses_correctly(monkeypatch, dummy_tracking_model):
    """Should return PODataParsed with parsed items and attributes assigned"""

    mock_text = "a\nb\nc"
    mock_items = [{"x": 1}, {"x": 2}]

    helper = TxtHelper(dummy_tracking_model)
    helper.capacity = "3 KB"
    helper.document_type = "order"

    monkeypatch.setattr(helper, "extract_text", lambda: mock_text)
    mock_parse_func = MagicMock(return_value=mock_items)

    result = helper.parse_file_to_json(mock_parse_func)

    assert isinstance(result, PODataParsed)
    assert result.document_type == "order"
    assert result.po_number == "2"
    assert result.items == mock_items
    assert result.capacity == "3 KB"
    assert result.step_status == StatusEnum.SUCCESS
    mock_parse_func.assert_called_once_with(["a", "b", "c"])


def test_parse_file_to_json_handles_empty(monkeypatch, dummy_tracking_model):
    """Should handle empty text gracefully"""
    helper = TxtHelper(dummy_tracking_model)
    helper.capacity = "0 KB"
    helper.document_type = "order"

    monkeypatch.setattr(helper, "extract_text", lambda: "")
    mock_parse_func = MagicMock(return_value=[])

    result = helper.parse_file_to_json(mock_parse_func)

    assert isinstance(result, PODataParsed)
    assert result.po_number == "0"
    assert result.items == []
    assert result.capacity == "0 KB"


def test_extract_text_error_handling(monkeypatch, dummy_tracking_model):
    """Should raise exception when file reading fails"""
    mock_processor = MagicMock()
    mock_processor.source = "local"
    mock_processor.file_path = "nonexistent.txt"
    mock_processor._get_file_capacity.return_value = "1 KB"
    mock_processor._get_document_type.return_value = "order"

    monkeypatch.setattr("utils.ext_extraction.FileExtensionProcessor", lambda **_: mock_processor)
    helper = TxtHelper(dummy_tracking_model)

    with patch.object(builtins, "open", side_effect=FileNotFoundError):
        with pytest.raises(FileNotFoundError):
            helper.extract_text()

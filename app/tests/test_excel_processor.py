import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path

# Import đúng theo cấu trúc bạn cung cấp
from fastapi_celery.processors.file_processors.excel_processor import ExcelProcessor
from fastapi_celery.models.class_models import StatusEnum


@pytest.fixture
def mock_tracking_model():
    """Create a fake TrackingModel using MagicMock, no real files needed."""
    mock = MagicMock()
    mock.file_path = Path("/fake/path/dummy.xlsx")
    mock.request_id = "req-123"
    return mock


@pytest.fixture
def processor(mock_tracking_model):
    """Initialize ExcelProcessor but mock ExcelHelper.__init__ to avoid file IO."""
    with patch("fastapi_celery.processors.file_processors.excel_processor.excel_helper.ExcelHelper.__init__", return_value=None):
        processor = ExcelProcessor(tracking_model=mock_tracking_model)
        processor.tracking_model = mock_tracking_model
        processor.rows = []
        processor.document_type = "order"
        processor.capacity = "full"
        return processor


def test_parse_only_metadata(processor):
    """In case of metadata only."""
    processor.rows = [
        ["PO Number：12345"],
        ["Date：2025-10-17"]
    ]

    def fake_extract_metadata(row):
        if "PO Number" in row[0]:
            return {"PO Number": "12345"}
        if "Date" in row[0]:
            return {"Date": "2025-10-17"}
        return {}

    processor.extract_metadata = fake_extract_metadata

    result = processor.parse_file_to_json()

    assert result.metadata == {"PO Number": "12345", "Date": "2025-10-17"}
    assert result.items == []
    assert result.step_status == StatusEnum.SUCCESS
    assert "dummy.xlsx" in str(result.original_file_path)


def test_parse_with_table(processor):
    """In case of data sheet."""
    processor.rows = [
        ["Item", "Qty"],
        ["Pen", "10"],
        ["Book", "20"]
    ]

    processor.extract_metadata = lambda row: {}

    result = processor.parse_file_to_json()

    assert len(result.items) == 2
    assert result.items[0]["Item"] == "Pen"
    assert result.items[1]["Qty"] == "20"
    assert result.metadata == {}
    assert result.step_status == StatusEnum.SUCCESS


def test_parse_mixed_metadata_and_table(processor):
    processor.rows = [
        ["PO Number：8888"],
        ["Item", "Qty"],
        ["A", "1"],
        ["B", "2"],
        ["Comment：Done"]
    ]

    def fake_extract_metadata(row):
        text = row[0]
        if "PO Number" in text:
            return {"PO Number": "8888"}
        if "Comment" in text:
            return {"Comment": "Done"}
        return {}

    processor.extract_metadata = fake_extract_metadata

    result = processor.parse_file_to_json()

    assert result.metadata == {"PO Number": "8888", "Comment": "Done"}
    assert len(result.items) == 2
    assert result.items[0]["Item"] == "A"
    assert result.items[1]["Qty"] == "2"


def test_parse_metadata_between_tables(processor):
    processor.rows = [
        ["Header1", "Header2"],
        ["x1", "y1"],
        ["Info：mid-table"],
        ["H2-1", "H2-2"],
        ["x2", "y2"]
    ]

    def fake_extract_metadata(row):
        if "Info" in row[0]:
            return {"Info": "mid-table"}
        return {}

    processor.extract_metadata = fake_extract_metadata

    result = processor.parse_file_to_json()

    assert "Info" in result.metadata
    assert any("Header1" in item for item in result.items)
    assert result.step_status == StatusEnum.SUCCESS


def test_parse_empty_rows(processor):
    processor.rows = []
    processor.extract_metadata = lambda row: {}

    result = processor.parse_file_to_json()

    assert result.metadata == {}
    assert result.items == []
    assert result.step_status == StatusEnum.SUCCESS


def test_logger_exists(processor):
    from fastapi_celery.processors.file_processors import excel_processor
    assert hasattr(excel_processor, "logger")
    assert excel_processor.logger is not None
